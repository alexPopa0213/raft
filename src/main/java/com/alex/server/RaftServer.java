package com.alex.server;

import com.alex.raft.*;
import com.alex.raft.client.ClientReply;
import com.alex.raft.client.ClientRequest;
import com.alex.server.heartbeat.udp.HeartbeatClusterRefresherTimerTask;
import com.alex.server.heartbeat.udp.HeartbeatListenerTimerTask;
import com.alex.server.heartbeat.udp.HeartbeatPublisherTimerTask;
import com.alex.server.model.LogEntry;
import com.alex.server.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ProtocolStringList;
import com.sun.net.httpserver.HttpServer;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.Logger;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alex.raft.RaftServiceGrpc.newBlockingStub;
import static com.alex.server.config.ApplicationProperties.*;
import static com.alex.server.model.ServerState.*;
import static com.alex.server.serdes.ProtoBuilderUtil.toProto;
import static com.alex.server.util.Utils.findMissingEntries;
import static com.alex.server.util.Utils.removeConflictingAndExtraEntries;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.err;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toConcurrentMap;
import static org.apache.logging.log4j.LogManager.getLogger;

public class RaftServer implements Identifiable {
    private static final Logger LOGGER = getLogger(RaftServer.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private ServerState state;

    private final String address;
    private final int port;
    private final int rest_port;
    private final String id;
    private Server server;

    private final Map<String, Timestamp> cluster = new ConcurrentHashMap<>();

    private final Map<String, ManagedChannel> managedChannelMap = new ConcurrentHashMap<>();
    private final Map<String, RaftServiceGrpc.RaftServiceBlockingStub> stubs = new ConcurrentHashMap<>();

    private DB db;

    /*
    state persisted to mapDB
     */
    private Atomic.Long currentTerm;
    private Atomic.String votedFor;
    private List<LogEntry> log;

    /*
    volatile state
     */
    private int commitIndex = 0;
    private int lastApplied = 0;
    private String leaderId;

    private int receivedVotes = 0;

    /*
    leader specific volatile state
     */
    private Map<String, Integer> nextIndex;
    private Map<String, Integer> matchIndex;

    private final int electionTimeOut;
    private final int leaderHeartbeatTimeout;

    private final AtomicBoolean leaderAlreadyElected = new AtomicBoolean(false);

    private final ExecutorService executorService = newCachedThreadPool();
    private final Timer timerQueue = new Timer(false);

    private final Object LOCK = new Object();

    public RaftServer(int port, int rest_port, String id, List<LogEntry> precondition, ElectionTimeoutProperties electionTimeoutProperties) {
        this.port = port;
        this.rest_port = rest_port;
        this.id = id;
        initPersistentState(precondition);
        state = FOLLOWER;
        electionTimeOut = generateRandomElectionTimeout(electionTimeoutProperties);
        leaderHeartbeatTimeout = electionTimeOut / LEADER_HEARTBEAT_SPLIT;
        address = getAddress() + ':' + port;
    }

    private String getAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            LOGGER.error("Could not retrieve address. Falling back to localhost.");
        }
        return "localhost";
    }

    private int generateRandomElectionTimeout(ElectionTimeoutProperties electionTimeoutProperties) {
        return new Random().nextInt(
                electionTimeoutProperties.getElectionTimeoutUpperBound() - electionTimeoutProperties.getElectionTimeoutLowerBound())
                + electionTimeoutProperties.getElectionTimeoutLowerBound();
    }

    private void initPersistentState(List<LogEntry> precondition) {
        //todo: wrap persistent state inside an object
        String dbName = DB_NAME_PREFIX + id;
        db = DBMaker.fileDB(dbName).checksumHeaderBypass().closeOnJvmShutdown().make();
        log = db.indexTreeList(id + LOG_NAME_SUFFIX, new LogEntrySerializer()).createOrOpen();
        if (log.isEmpty()) {
            //so actual log will start at index = 1
            log.add(new LogEntry(0L, VOID_VALUE, 0));
            log.addAll(precondition);
        }
        votedFor = db.atomicString(id + VOTED_FOR_SUFFIX).createOrOpen();
        currentTerm = db.atomicLong(id + CURRENT_TERM_SUFFIX).createOrOpen();
    }

    public void start() {
        server = ServerBuilder.forPort(port)
                .addService(new RaftServiceImpl())
                .build();
        try {
            server.start();
        } catch (IOException ex) {
            LOGGER.error("Server could not be started", ex);
            return;
        }
        LOGGER.info("Server {} started at: {}", id, address);
        LOGGER.info("Election timeout randomly chosen: {}", electionTimeOut);
        enableHeartbeat();
        preventElections();
        handleElections();
        notifyReplication();
        enableRestEndpoints();
        addShutdownHook();
    }

    private void enableRestEndpoints() {
        HttpServer server;
        try {
            server = HttpServer.create(new InetSocketAddress(rest_port), 0);
            addGetEntriesEndpoint(server);
            addGetServerStateEndpoint(server);
            addGetElectionTimeoutEndpoint(server);
            server.setExecutor(executorService);
            server.start();
        } catch (IOException e) {
            LOGGER.warn("Error enabling REST endpoints");
        }
    }

    private void addGetElectionTimeoutEndpoint(HttpServer server) {
        server.createContext("/api/timeout", (httpHandler -> {
            if (httpHandler.getRequestMethod().equals("GET")) {
                httpHandler.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                httpHandler.getResponseHeaders().add(CONTENT_TYPE, "application/json");
                httpHandler.sendResponseHeaders(200, 0);
                OutputStream output = httpHandler.getResponseBody();
                output.write(OBJECT_MAPPER.writeValueAsBytes(electionTimeOut));
                output.flush();
            } else {
                httpHandler.sendResponseHeaders(405, -1);
            }
            httpHandler.close();
        }));
    }

    private void addGetServerStateEndpoint(HttpServer server) {
        server.createContext("/api/state", (httpHandler -> {

            if (httpHandler.getRequestMethod().equals("GET")) {
                String serverState;
                synchronized (LOCK) {
                    serverState = state.toString();
                }
                httpHandler.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                httpHandler.getResponseHeaders().add(CONTENT_TYPE, "application/json");
                httpHandler.sendResponseHeaders(200, 0);
                OutputStream output = httpHandler.getResponseBody();
                output.write(OBJECT_MAPPER.writeValueAsBytes(serverState));
                output.flush();
            } else {
                httpHandler.sendResponseHeaders(405, -1);
            }
            httpHandler.close();
        }));
    }

    private void addGetEntriesEndpoint(HttpServer server) {
        server.createContext("/api/entries", (httpHandler -> {

            if (httpHandler.getRequestMethod().equals("GET")) {
                List<LogEntry> entries;
                synchronized (LOCK) {
                    entries = new ArrayList<>(log);
                }
                httpHandler.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                httpHandler.getResponseHeaders().add(CONTENT_TYPE, "application/json");
                httpHandler.sendResponseHeaders(200, 0);
                OutputStream output = httpHandler.getResponseBody();
                output.write(OBJECT_MAPPER.writeValueAsBytes(entries));
                output.flush();
            } else {
                httpHandler.sendResponseHeaders(405, -1);
            }
            httpHandler.close();
        }));
    }

    private void enableHeartbeat() {
        listenHeartbeats();
        sendHeartbeats();
        refreshCluster();
    }

    private void listenHeartbeats() {
        timerQueue.schedule(new HeartbeatListenerTimerTask(cluster, address), 0, LISTEN_UDP_HEARTBEATS_INTERVAL);
    }

    private void sendHeartbeats() {
        timerQueue.schedule(new HeartbeatPublisherTimerTask(address), 0, SEND_UDP_HEARTBEATS_INTERVAL);
    }

    private void refreshCluster() {
        timerQueue.schedule(new HeartbeatClusterRefresherTimerTask(cluster), 0, UDP_REFRESH_CLUSTER_INTERVAL);
    }

    private void preventElections() {
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                sendHeartbeatRPC();
            }
        };
        timerQueue.schedule(timerTask, 0, leaderHeartbeatTimeout);
    }

    private void notifyReplication() {
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                notifyWhenReplicationIsDone();
            }
        };
        timerQueue.schedule(timerTask, 0, leaderHeartbeatTimeout);
    }

    private void notifyWhenReplicationIsDone() {
        synchronized (LOCK) {
            if (state == LEADER) {
                int majority = (int) round(((double) (cluster.size() + 1)) / 2 + 0.5);
                int lastLogIndex = log.size() - 1;
                int match = 0;
                for (String server : cluster.keySet()) {
                    if (matchIndex.containsKey(server) && matchIndex.get(server) == lastLogIndex) {
                        match++;
                        if (match >= majority) {
                            LOGGER.debug("Data replicated on a majority of servers.");
                            LOCK.notifyAll();
                        }
                    }
                }
            }
        }
    }

    private void sendHeartbeatRPC() {
        synchronized (LOCK) {
            if (state != LEADER) {
                return;
            }
        }
        for (final String server : cluster.keySet()) {
            executorService.execute(() -> {
                long start = currentTimeMillis();
                LOGGER.trace("Sending heartbeat RPC to: {}", server);
                sendAppendEntriesRPC(server);
                LOGGER.debug("Heartbeat rpc to {} took: {} ms.", server, currentTimeMillis() - start);
            });
        }
    }

    public void sendAppendEntriesRPC(String server) {
        final ManagedChannel managedChannel;
        final RaftServiceGrpc.RaftServiceBlockingStub blockingStub;
        AppendEntriesRequest appendEntriesRequest;

        synchronized (LOCK) {
            if (state != LEADER) {
                LOGGER.debug("I'm not LEADER anymore, will not send any more heartbeats!.");
                return;
            }
            leaderAlreadyElected.set(true);
            List<LogEntry> entries = new ArrayList<>();
            int lastLogIndex = log.size() - 1;
            if (!nextIndex.containsKey(server)) {
                nextIndex.put(server, lastLogIndex + 1);
                LOGGER.debug("Server {} is new so his nextIndex is: {}", server, log.size());
            }
            if (lastLogIndex >= nextIndex.get(server)) {
                LOGGER.debug("Sending missing entries. Next Index for Server _{} is: {} and lastLogIndex is: {}",
                        server, nextIndex.get(server), lastLogIndex);
                LOGGER.debug("Sending missing entries: {}", log.subList(nextIndex.get(server), log.size()));
                entries.addAll(log.subList(nextIndex.get(server), log.size()));
            }
            int prevLogIndex = nextIndex.get(server) - 1;
            long prevLogTerm = prevLogIndex == -1 ? -1 : log.get(prevLogIndex).getTerm();
            appendEntriesRequest = AppendEntriesRequest.newBuilder()
                    .setLeaderId(address)
                    .setTerm(currentTerm.get())
                    .setLeaderCommitIndex(commitIndex)
                    .setPrevLogIndex(prevLogIndex)
                    .setPrevLogTerm(prevLogTerm)
                    .addAllEntries(toProto(entries))
                    .build();
            try {
                managedChannel = managedChannelMap.computeIfAbsent(server, this::buildChannel);
                blockingStub = stubs.computeIfAbsent(server, integer -> newBlockingStub(managedChannel));
                AppendEntriesReply appendEntriesReply = blockingStub.appendEntries(appendEntriesRequest);
                if (!appendEntriesReply.getSuccess()) {
                    LOGGER.debug("Heartbeat to server {} failed.", server);
                    checkAndUpdateTerm(appendEntriesReply.getTerm());
                    if (state == LEADER) {
                        Integer idx = nextIndex.get(server) - 1;
                        nextIndex.put(server, idx);
                    }
                } else {
                    int lastLogIndexx = log.size() - 1;
                    nextIndex.put(server, lastLogIndexx + 1);
                    matchIndex.put(server, lastLogIndexx);
                }
            } catch (StatusRuntimeException sre) {
                LOGGER.warn("RPC failed: {}, {}", sre.getStatus(), sre.getMessage());
                removeServerIfUnavailable(sre.getStatus(), server);
            } catch (Exception e) {
                LOGGER.error("ERROR calling appendEntries RPC entries", e);
            }
        }
    }

    private void handleElections() {
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                synchronized (LOCK) {
                    if (leaderAlreadyElected.getAndSet(false)) {
                        return;
                    }
                }
                startElection();
            }
        };
        timerQueue.schedule(timerTask, electionTimeOut, electionTimeOut);
    }

    private void startElection() {
        synchronized (LOCK) {
            if (!leaderAlreadyElected.get()) {
                state = CANDIDATE;
                currentTerm.set(currentTerm.get() + 1);
                votedFor.set(null);
                receivedVotes = 0;
            }
        }

        for (final String address : cluster.keySet()) {
            executorService.execute(() -> sendRequestVoteRPC(address));
        }
    }

    private void sendRequestVoteRPC(String address) {
        synchronized (LOCK) {
            if (state != CANDIDATE || leaderAlreadyElected.get()) {
                return;
            }
        }
        LOGGER.debug("Sending RequestVoteRpc to:" + address);
        RequestVoteReply requestVoteReply = requestVote(address);
        synchronized (LOCK) {
            if (nonNull(requestVoteReply)) {
                if (requestVoteReply.getVoteGranted()) {
                    LOGGER.debug("Received vote from: {}", address);
                    receivedVotes += 1;
                    if (cluster.size() + 1 >= minimumClusterSize) {
                        if (hasMajorityOfVotes() && cluster.size() + 1 >= minimumClusterSize) {
                            LOGGER.debug("I am now LEADER of term: {}", currentTerm.get());
                            state = LEADER;
                            receivedVotes = 0;
                            int lastLogIndex = log.size() - 1;
                            nextIndex = cluster.entrySet().stream().collect(toConcurrentMap(Map.Entry::getKey, v -> lastLogIndex + 1));
                            matchIndex = cluster.entrySet().stream().collect(toConcurrentMap(Map.Entry::getKey, v -> 0));
                        }
                    } else {
                        LOGGER.debug("Cluster does not have the minimum size: {}", minimumClusterSize);
                    }
                } else {
                    checkAndUpdateTerm(requestVoteReply.getTerm());
                    leaderAlreadyElected.set(true);
                }
            }
        }
    }

    public RequestVoteReply requestVote(String address) {
        RequestVoteRequest requestVoteRequest;
        RequestVoteReply requestVoteReply = null;
        final ManagedChannel managedChannel;
        final RaftServiceGrpc.RaftServiceBlockingStub blockingStub;
        synchronized (LOCK) {
            int lastLogIndex = log.size() - 1;
            long lastLogTerm = log.get(lastLogIndex).getTerm();
            requestVoteRequest = RequestVoteRequest.newBuilder()
                    .setTerm(currentTerm.get())
                    .setCandidateIp(this.address)
                    .setLastLogIndex(lastLogIndex)
                    .setLastLogTerm(lastLogTerm)
                    .build();
        }
        try {
            managedChannel = managedChannelMap.computeIfAbsent(address, this::buildChannel);
            blockingStub = stubs.computeIfAbsent(address, integer -> newBlockingStub(managedChannel));
            requestVoteReply = blockingStub.requestVote(requestVoteRequest);
        } catch (StatusRuntimeException sre) {
            LOGGER.warn("RPC failed: {}, {}", sre.getStatus(), sre.getMessage());
            removeServerIfUnavailable(sre.getStatus(), address);
        }
        return requestVoteReply;
    }

    private ManagedChannel buildChannel(String address) {
        ManagedChannel managedChannel = ManagedChannelBuilder.forTarget(address)
                .executor(executorService)
                .usePlaintext()
                .build();
        LOGGER.debug("Created managedChannel for address {}.", address);
        return managedChannel;
    }

    private void checkAndUpdateTerm(long newTerm) {
        synchronized (LOCK) {
            if (newTerm > currentTerm.get()) {
                LOGGER.debug("Old term is: {}", currentTerm.get());
                currentTerm.set(newTerm);
                LOGGER.debug("Updated term is: {}", currentTerm.get());
                state = FOLLOWER;
                votedFor.set(null);
                leaderAlreadyElected.set(true);
            }
        }
    }

    private void removeServerIfUnavailable(Status status, String address) {
        LOGGER.debug("RPC failed and server {} will be removed from cluster.", address);
        if (status.getCode().equals(Status.Code.UNAVAILABLE)) { //maybe retry first?
            cluster.remove(address);
            if (managedChannelMap.containsKey(address)) {
                managedChannelMap.get(address).shutdownNow();
                managedChannelMap.remove(address);
                stubs.remove(address);
            }
        }
    }

    private boolean hasMajorityOfVotes() {
        int majority = (int) round(((double) (cluster.size() + 1)) / 2 + 0.5);
        LOGGER.debug("I received {} votes and the majority is: {}.", receivedVotes + 1, majority);
        return receivedVotes + 1 >= majority;
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(5, SECONDS);
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                db.close();
                managedChannelMap.forEach((integer, managedChannel) -> managedChannel.shutdown());
                this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(err);
            }
            err.println("*** server shut down");
        }));
    }

    public List<LogEntry> getLog() {
        return log;
    }

    @Override
    public String getId() {
        return id;
    }

    class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {

        @Override
        public void sendCommands(ClientRequest request, StreamObserver<ClientReply> responseObserver) {
            ClientReply.Builder builder = ClientReply.newBuilder();
            ProtocolStringList commandsList = request.getCommandsList();
            LOGGER.debug("Received commands: {}", commandsList);
            boolean success = false;
            boolean shouldRedirect;
            synchronized (LOCK) {
                shouldRedirect = state != LEADER && leaderId != null;
            }
            if (shouldRedirect) {
                final ManagedChannel managedChannel;
                final RaftServiceGrpc.RaftServiceBlockingStub blockingStub;
                LOGGER.debug("Redirecting request to leader {}", leaderId);
                ClientRequest clientRequest = ClientRequest.newBuilder()
                        .addAllCommands(commandsList)
                        .build();
                managedChannel = managedChannelMap.computeIfAbsent(leaderId, RaftServer.this::buildChannel);
                blockingStub = stubs.computeIfAbsent(leaderId, integer -> newBlockingStub(managedChannel));
                ClientReply clientReply = blockingStub.sendCommands(clientRequest);
                success = clientReply.getSuccess();
            } else {
                if (cluster.size() + 1 >= minimumClusterSize) {
                    synchronized (LOCK) {
                        if (state == LEADER) {
                            int index = log.get(log.size() - 1).getIndex();
                            for (String command : commandsList) {
                                index++;
                                log.add(new LogEntry(currentTerm.get(), command, index));
                            }
                            LOGGER.debug("Log is now: {}", log);
                            try {
                                LOCK.wait();
                                success = true;
                            } catch (InterruptedException e) {
                                LOGGER.error(e);
                            }
                        }
                    }
                } else {
                    LOGGER.debug("Cluster does not have the minimum size: {}", minimumClusterSize);
                    LOGGER.debug("Client request declined.");
                }
            }
            builder.setSuccess(success);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesReply> responseObserver) {
            AppendEntriesReply.Builder builder = AppendEntriesReply.newBuilder();
            final long myTerm;
            LOGGER.debug("Received append entries RPC from {}. Request is: {}.", request.getLeaderId(), request.toString());
            synchronized (LOCK) {
                myTerm = currentTerm.get();
                int prevLogIndex = request.getPrevLogIndex();
                long prevLogTerm = request.getPrevLogTerm();
                int lastLogIndex = log.size() - 1;
                if (request.getTerm() < myTerm
                        || prevLogIndex > lastLogIndex
                        || (prevLogTerm != -1 && prevLogTerm != log.get(prevLogIndex).getTerm())) {
                    builder.setSuccess(false);
                    LOGGER.debug("Denied appendEntries RPC, prevLogIndex is {} and my lastLogIndex {}", prevLogIndex, lastLogIndex);
                } else {
                    log = removeConflictingAndExtraEntries(log, request.getEntriesList(), request.getPrevLogIndex());
                    log.addAll(findMissingEntries(log, request.getEntriesList()));
                    if (request.getLeaderCommitIndex() > commitIndex) {
                        commitIndex = min(request.getLeaderCommitIndex(), log.get(lastLogIndex).getIndex());
                    }
                    builder.setSuccess(true);
                }
                state = FOLLOWER;
                currentTerm.set(request.getTerm());
                leaderAlreadyElected.set(true);
                leaderId = request.getLeaderId();
                LOGGER.debug("Received appendEntries RPC from {}. Current state is: {}", request.getLeaderId(), state.toString());
            }
            builder.setTerm(myTerm);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteReply> responseObserver) {
            String candidateAddress = request.getCandidateIp();
            LOGGER.debug("Received RequestVoteRPC from: {}", candidateAddress);
            RequestVoteReply.Builder builder = RequestVoteReply.newBuilder();
            if (!cluster.containsKey(candidateAddress)) {
                builder.setVoteGranted(false);
                LOGGER.debug("Denied requestVote from {} because it is not part of the cluster.", candidateAddress);
            } else {
                synchronized (LOCK) {
                    long myTerm = currentTerm.get();
                    LOGGER.debug("Current term is: {} and received term is: {}", myTerm, request.getTerm());
                    builder.setTerm(myTerm);
                    int lastLogIndex = log.size() - 1;
                    long lastLogTerm = log.get(lastLogIndex).getTerm();
                    if (request.getTerm() < myTerm) {
                        builder.setVoteGranted(false);
                        LOGGER.debug("Vote NOT granted to {} because term was lower.", candidateAddress);
                    } else if (isNull(votedFor.get())) {
                        if (request.getLastLogTerm() >= lastLogTerm && request.getLastLogIndex() >= lastLogIndex) {
                            builder.setVoteGranted(true);
                            votedFor.set(candidateAddress);
                            leaderAlreadyElected.set(true);
                            LOGGER.debug("Vote granted to {}", candidateAddress);
                            state = FOLLOWER;
                        } else {
                            builder.setVoteGranted(false);
                            LOGGER.debug("Vote NOT granted to {} because it's log was not up-to-date.", candidateAddress);
                        }
                    } else {
                        builder.setVoteGranted(false);
                        LOGGER.debug("Vote NOT granted to {} because I already voted for {}.", candidateAddress, votedFor.get());
                    }
                }
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }
    }

}
