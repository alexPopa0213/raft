package com.alex.server;

import com.alex.raft.*;
import com.alex.raft.client.ClientReply;
import com.alex.raft.client.ClientRequest;
import com.alex.server.heartbeat.udp.HeartbeatClusterRefresherTimerTask;
import com.alex.server.heartbeat.udp.HeartbeatListenerTimerTask;
import com.alex.server.heartbeat.udp.HeartbeatPublisherTimerTask;
import com.alex.server.model.Identifiable;
import com.alex.server.model.LogEntry;
import com.alex.server.model.LogEntrySerializer;
import com.alex.server.model.ServerState;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.Logger;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alex.raft.RaftServiceGrpc.newBlockingStub;
import static com.alex.server.config.ApplicationProperties.*;
import static com.alex.server.model.ServerState.*;
import static com.alex.server.util.Utils.findMissingEntries;
import static com.alex.server.util.Utils.removeConflictingEntries;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.err;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.logging.log4j.LogManager.getLogger;

public class RaftServer implements Identifiable {
    private static final Logger LOGGER = getLogger(RaftServer.class);

    private ServerState state;

    private final int port;
    private final String id;
    private Server server;

    private final Map<Integer, Timestamp> cluster = new ConcurrentHashMap<>();

    private final Map<Integer, ManagedChannel> managedChannelMap = new ConcurrentHashMap<>();
    private final Map<Integer, RaftServiceGrpc.RaftServiceBlockingStub> stubs = new ConcurrentHashMap<>();

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

    private int receivedVotes = 0;

    /*
    leader specific volatile state
     */
    private Map<Integer, Integer> nextIndex;
    private Map<Integer, Integer> matchIndex;

    private final int electionTimeOut;
    private final int leaderHeartbeatTimeout;

    private final AtomicBoolean leaderAlreadyElected = new AtomicBoolean(false);

    private final ExecutorService executorService = newCachedThreadPool();
    private final Timer timerQueue = new Timer(false);

    private final Object LOCK = new Object();

    public RaftServer(int port, String id) {
        this.port = port;
        this.id = id;
        initPersistentState();
        state = FOLLOWER;
        electionTimeOut = generateRandomElectionTimeout();
        leaderHeartbeatTimeout = electionTimeOut / LEADER_HEARTBEAT_SPLIT;
    }

    private int generateRandomElectionTimeout() {
        return new Random().nextInt(ELECTION_TIMER_UPPER_BOUND - ELECTION_TIMER_LOWER_BOUND) + ELECTION_TIMER_LOWER_BOUND;
    }

    private void initPersistentState() {
        //todo: wrap persistent state inside an object
        String dbName = "db_" + id;
        db = DBMaker.fileDB(dbName).checksumHeaderBypass().closeOnJvmShutdown().make();
        log = db.indexTreeList(id + "_log", new LogEntrySerializer()).createOrOpen();
        votedFor = db.atomicString(id + "_votedFor").createOrOpen();
        currentTerm = db.atomicLong(id + "_current_term").createOrOpen();
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
        LOGGER.info("Server {} started on port {}", id, port);
        LOGGER.info("Election timeout randomly chosen: {}", electionTimeOut);
        enableHeartbeat();
        preventElections();
        handleElections();
        addShutdownHook();
    }

    private void enableHeartbeat() {
        listenHeartbeats();
        sendHeartbeats();
        refreshCluster();
    }

    private void listenHeartbeats() {
        timerQueue.schedule(new HeartbeatListenerTimerTask(cluster, port), 0, LISTEN_UDP_HEARTBEATS_INTERVAL);
    }

    private void sendHeartbeats() {
        timerQueue.schedule(new HeartbeatPublisherTimerTask(port), 0, SEND_UDP_HEARTBEATS_INTERVAL);
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

    private void sendHeartbeatRPC() {
        synchronized (LOCK) {
            if (state != LEADER) {
                return;
            }
        }
        for (final Integer server : cluster.keySet()) {
            executorService.execute(() -> {
                long start = currentTimeMillis();
                LOGGER.trace("Sending heartbeat RPC to: {}", server);
                sendEmptyAppendEntriesRPC(server);
                LOGGER.debug("Heartbeat rpc to {} took: {} ms.", server, currentTimeMillis() - start);
            });
        }
    }

    public void sendEmptyAppendEntriesRPC(int port) {
        final ManagedChannel managedChannel;
        final RaftServiceGrpc.RaftServiceBlockingStub blockingStub;
        AppendEntriesRequest appendEntriesRequest;

        synchronized (LOCK) {
            if (state != LEADER) {
                LOGGER.debug("I'm not LEADER anymore, will not send any more heartbeats!.");
                return;
            }
            leaderAlreadyElected.set(true);
            //index before appending logs from client
            int prevLogIndex = log.size() - 1;
            long prevLogTerm = prevLogIndex != -1 ? log.get(prevLogIndex).getTerm() : -1;
            //append log entries from client, then send request
            appendEntriesRequest = AppendEntriesRequest.newBuilder()
                    .setLeaderId(id)
                    .setTerm(currentTerm.get())
                    .setLeaderCommitIndex(commitIndex)
                    .setPrevLogIndex(prevLogIndex)
                    .setPrevLogTerm(prevLogTerm)
                    .addAllEntries(new ArrayList<>())
                    .build();
        }
        try {
            managedChannel = managedChannelMap.computeIfAbsent(port, this::buildChannel);
            blockingStub = stubs.computeIfAbsent(port, integer -> newBlockingStub(managedChannel));
            AppendEntriesReply appendEntriesReply = blockingStub.appendEntries(appendEntriesRequest);
            if (!appendEntriesReply.getSuccess()) {
                LOGGER.debug("Heartbeat to server {} failed.", port);
                checkAndUpdateTerm(appendEntriesReply.getTerm());
            }
        } catch (StatusRuntimeException sre) {
            LOGGER.warn("RPC failed: {}, {}", sre.getStatus(), sre.getMessage());
            removeServerIfUnavailable(sre.getStatus(), port);
        } catch (Exception e) {
            LOGGER.error("ERROR calling appendEntries RPC entries", e);
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
        LOGGER.debug("Started election.");
        synchronized (LOCK) {
            state = CANDIDATE;
            currentTerm.set(currentTerm.get() + 1);
            votedFor.set(null);
            receivedVotes = 0;
        }

        for (final Integer server : cluster.keySet()) {
            executorService.execute(() -> {
                synchronized (LOCK) {
                    if (state != CANDIDATE) {
                        return;
                    }
                }
                LOGGER.debug("Sending RequestVoteRpc to:" + server);
                RequestVoteReply requestVoteReply = requestVote(server);
                synchronized (LOCK) {
                    if (nonNull(requestVoteReply)) {
                        if (requestVoteReply.getVoteGranted()) {
                            LOGGER.debug("Received vote from: {}", server);
                            receivedVotes += 1;
                            if (hasMajorityOfVotes()) {
                                LOGGER.debug("I am now LEADER of term: {}", currentTerm.get());
                                state = LEADER;
                                receivedVotes = 0;
                            }
                        } else {
                            checkAndUpdateTerm(requestVoteReply.getTerm());
                            leaderAlreadyElected.set(true);
                        }
                    }
                }
            });
        }
    }

    public RequestVoteReply requestVote(Integer port) {
        RequestVoteRequest requestVoteRequest;
        RequestVoteReply requestVoteReply = null;
        final ManagedChannel managedChannel;
        final RaftServiceGrpc.RaftServiceBlockingStub blockingStub;
        synchronized (LOCK) {
            int lastLogIndex = log.size() - 1;
            long lastLogTerm = lastLogIndex != -1 ? log.get(lastLogIndex).getTerm() : -1;
            requestVoteRequest = RequestVoteRequest.newBuilder()
                    .setTerm(currentTerm.get())
                    .setCandidateId(id)
                    .setLastLogIndex(lastLogIndex)
                    .setLastLogTerm(lastLogTerm)
                    .build();
        }
        try {
            managedChannel = managedChannelMap.computeIfAbsent(port, this::buildChannel);
            blockingStub = stubs.computeIfAbsent(port, integer -> newBlockingStub(managedChannel));
            requestVoteReply = blockingStub.requestVote(requestVoteRequest);
        } catch (StatusRuntimeException sre) {
            LOGGER.warn("RPC failed: {}, {}", sre.getStatus(), sre.getMessage());
            removeServerIfUnavailable(sre.getStatus(), port);
        }
        return requestVoteReply;
    }

    private ManagedChannel buildChannel(Integer port) {
        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress("127.0.0.1", port)
                .executor(executorService)
                .usePlaintext()
                .build();
        LOGGER.debug("Created managedChannel for port {}.", port);
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
            }
        }
    }

    private void removeServerIfUnavailable(Status status, Integer port) {
        if (status.getCode().equals(Status.Code.UNAVAILABLE)) { //maybe retry first?
            cluster.remove(port);
            managedChannelMap.remove(port);
            stubs.remove(port);
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

    @Override
    public String getId() {
        return id;
    }

    class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {

        @Override
        public void sendCommands(ClientRequest request, StreamObserver<ClientReply> responseObserver) {
            ClientReply.Builder builder = ClientReply.newBuilder();
            LOGGER.debug("Received commands: {}", request.getCommandsList());
            builder.setSuccess(true);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesReply> responseObserver) {
            AppendEntriesReply.Builder builder = AppendEntriesReply.newBuilder();
            final long myTerm;
            LOGGER.debug("Received heartbeat RPC from {}.", request.getLeaderId());
            synchronized (LOCK) {
                myTerm = currentTerm.get();
                if (isHeartBeat(request) && request.getTerm() >= myTerm) {
                    state = FOLLOWER;
                    currentTerm.set(request.getTerm());
                    leaderAlreadyElected.set(true);
                    builder.setSuccess(true);
                    LOGGER.debug("Accepted heartbeat RPC from {}. Current state is: {}", request.getLeaderId(), state.toString());
                } else {
                    int prevLogIndex = request.getPrevLogIndex();
                    if (request.getTerm() < myTerm
                            || prevLogIndex >= log.size()
                            || log.get(prevLogIndex).getTerm() != request.getPrevLogTerm()) {
                        builder.setSuccess(false);
                    } else {
                        log = removeConflictingEntries(log, request.getEntriesList());
                        log.addAll(findMissingEntries(log, request.getEntriesList()));
                        if (request.getLeaderCommitIndex() > commitIndex) {
                            commitIndex = min(request.getLeaderCommitIndex(), log.get(log.size() - 1).getIndex());
                        }
                        builder.setSuccess(true);
                    }
                }
            }
            builder.setTerm(myTerm);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        private boolean isHeartBeat(AppendEntriesRequest request) {
            return request.getEntriesCount() == 0;
        }

        @Override
        public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteReply> responseObserver) {
            LOGGER.debug("Received RequestVoteRPC from: {}", request.getCandidateId());
            RequestVoteReply.Builder builder = RequestVoteReply.newBuilder();
            synchronized (LOCK) {
                long myTerm = currentTerm.get();
                LOGGER.debug("Current term is: {} and received term is: {}", myTerm, request.getTerm());
                builder.setTerm(myTerm);
                int lastLogIndex = log.size() - 1;
                long lastLogTerm = lastLogIndex != -1 ? log.get(lastLogIndex).getTerm() : -1;
                if (request.getTerm() < myTerm) {
                    builder.setVoteGranted(false);
                    LOGGER.debug("Vote NOT granted to {} because term was lower.", request.getCandidateId());
                } else if (isNull(votedFor.get())) {
                    if (request.getLastLogTerm() >= lastLogTerm && request.getLastLogIndex() >= lastLogIndex) {
                        builder.setVoteGranted(true);
                        votedFor.set(request.getCandidateId());
                        leaderAlreadyElected.set(true);
                        LOGGER.debug("Vote granted to {}", request.getCandidateId());
                    } else {
                        builder.setVoteGranted(false);
                        LOGGER.debug("Vote NOT granted to {} because it's log was not up-to-date.", request.getCandidateId());
                    }
                } else {
                    builder.setVoteGranted(false);
                    LOGGER.debug("Vote NOT granted to {} because I already voted for {}.", request.getCandidateId(), votedFor.get());
                }
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }
    }

}
