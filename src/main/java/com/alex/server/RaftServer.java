package com.alex.server;

import com.alex.raft.*;
import com.alex.server.heartbeat.HeartbeatClusterRefresherTimerTask;
import com.alex.server.heartbeat.HeartbeatListener;
import com.alex.server.heartbeat.HeartbeatPublisherTimerTask;
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
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static com.alex.raft.RaftServiceGrpc.newBlockingStub;
import static com.alex.server.model.ServerState.*;
import static java.lang.Integer.parseInt;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.err;
import static java.util.Arrays.stream;
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
    private Map<String, Integer> nextIndex;
    private Map<String, Integer> matchIndex;

    private static final int ELECTION_TIMER_UPPER_BOUND = 300;
    private static final int ELECTION_TIMER_LOWER_BOUND = 150;
    private final int electionTimeOut;
    private final int leaderHeartbeatTimeout;

    private long lastHeartBeatReceived;

    private final ExecutorService executorService = newCachedThreadPool();
    private final Timer timerQueue = new Timer(true);

    private final Object LOCK = new Object();

    public RaftServer(int port, String id) {
        this.port = port;
        this.id = id;
        initPersistentState(id);
        state = FOLLOWER;
        electionTimeOut = generateRandomElectionTimeout();
        leaderHeartbeatTimeout = electionTimeOut / 3;
    }

    private int generateRandomElectionTimeout() {
        return new Random().nextInt(ELECTION_TIMER_UPPER_BOUND - ELECTION_TIMER_LOWER_BOUND) + ELECTION_TIMER_LOWER_BOUND;
    }

    private void initPersistentState(String id) {
        String dbName = "db_" + id;
        db = DBMaker.fileDB(dbName).checksumHeaderBypass().closeOnJvmShutdown().make();
        log = db.indexTreeList(id + "_log", new LogEntrySerializer()).createOrOpen();
        votedFor = db.atomicString(id + "_votedFor").createOrOpen();
        currentTerm = db.atomicLong(id + "_current_term").createOrOpen();
        LOGGER.debug("Current term is: {}.", currentTerm.get());
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
        new Thread(new HeartbeatListener(cluster, port)).start();
    }

    private void sendHeartbeats() {
        timerQueue.schedule(new HeartbeatPublisherTimerTask(port), 0, 200);
    }

    private void refreshCluster() {
        timerQueue.schedule(new HeartbeatClusterRefresherTimerTask(cluster), 0, 1000);
    }

    private void preventElections() {
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                sendHeartbeatRPC();
            }
        };
        timerQueue.schedule(timerTask, leaderHeartbeatTimeout, leaderHeartbeatTimeout);
    }

    private void sendHeartbeatRPC() {
        synchronized (LOCK) {
            if (!serverStateIs(LEADER)) {
                return;
            }
        }
        for (final Integer server : cluster.keySet()) {
            executorService.execute(() -> {
                LOGGER.debug("Sending heartbeat RPC to: {}", server);
                sendEmptyAppendEntriesRPC(server);
            });
        }
    }

    public void sendEmptyAppendEntriesRPC(int port) {
        ManagedChannel managedChannel = null;
        AppendEntriesRequest appendEntriesRequest;

        synchronized (LOCK) {
            if (!serverStateIs(LEADER)) {
                return;
            }
            appendEntriesRequest = AppendEntriesRequest.newBuilder()
                    .setLeaderId(id)
                    .setTerm(currentTerm.get())
                    .setLeaderCommitIndex(commitIndex)
                    .setPrevLogIndex(0L)
                    .setPrevLogTerm(0L)
                    .addAllEntries(new ArrayList<>())
                    .build();
        }
        try {
            managedChannel = ManagedChannelBuilder.forAddress("localhost", port)
                    .usePlaintext()
                    .build();
            managedChannelMap.putIfAbsent(port, managedChannel);
            RaftServiceGrpc.RaftServiceBlockingStub blockingStub = newBlockingStub(managedChannel);
            AppendEntriesReply appendEntriesReply = blockingStub.appendEntries(appendEntriesRequest);
            if (!appendEntriesReply.getSuccess()) {
                LOGGER.debug("Heartbeat to server {} failed.", port);
                checkAndUpdateTerm(appendEntriesReply.getTerm());
            }
        } catch (StatusRuntimeException sre) {
            LOGGER.warn("RPC failed: {}, {}", sre.getStatus(), sre.getMessage());
        } catch (Exception e) {
            LOGGER.error("ERROR append entries", e);
        } finally {
            if (nonNull(managedChannel) && !managedChannel.isShutdown()) {
                managedChannel.shutdown();
            }
        }

    }

    private void handleElections() {
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                long currentTimeMillis = currentTimeMillis();
                synchronized (LOCK) {
                    if (serverStateIs(LEADER) || lastHeartBeatReceived + electionTimeOut >= currentTimeMillis) {
                        return;
                    }
                }
                LOGGER.debug("Didn't receive heartbeat for: {}", currentTimeMillis - lastHeartBeatReceived - electionTimeOut);
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
            //reset election timer
            lastHeartBeatReceived = currentTimeMillis();
        }

        for (final Integer server : cluster.keySet()) {
            executorService.execute(() -> {
                synchronized (LOCK) {
                    if (!serverStateIs(CANDIDATE)) {
                        return;
                    }
                }
                LOGGER.debug("Sending RequestVoteRpc to:" + server);
                RequestVoteReply requestVoteReply = requestVote(server);
                synchronized (LOCK) {
                    if (nonNull(requestVoteReply)) {
                        if (requestVoteReply.getVoteGranted()) {
                            LOGGER.debug("Received voted from: {}", server);
                            receivedVotes += 1;
                            if (hasMajorityOfVotes()) {
                                LOGGER.debug("I am now LEADER of term: {}", currentTerm.get());
                                state = LEADER;
                                receivedVotes = 0;
                            }
                        } else {
                            checkAndUpdateTerm(requestVoteReply.getTerm());
                        }
                    }
                }
            });
        }
    }

    public RequestVoteReply requestVote(Integer port) {
        RequestVoteRequest requestVoteRequest;
        RequestVoteReply requestVoteReply = null;
        ManagedChannel managedChannel = null;
        synchronized (LOCK) {
            int lastLogIndex = log.size() - 1;
            requestVoteRequest = RequestVoteRequest.newBuilder()
                    .setTerm(currentTerm.get())
                    .setCandidateId(id)
                    .setLastLogIndex(lastLogIndex)
                    .setLastLogTerm(lastLogIndex != -1 ? log.get(lastLogIndex).getTerm() : -1)
                    .build();
        }
        try {
            managedChannel = ManagedChannelBuilder.forAddress("localhost", port)
                    .usePlaintext()
                    .build();
            managedChannelMap.putIfAbsent(port, managedChannel);
            RaftServiceGrpc.RaftServiceBlockingStub blockingStub = newBlockingStub(managedChannel);
            requestVoteReply = blockingStub.requestVote(requestVoteRequest);
            managedChannel.shutdown();
        } catch (StatusRuntimeException sre) {
            LOGGER.warn("RPC failed: {}, {}", sre.getStatus(), sre.getMessage());
            removeServerIfUnavailable(sre.getStatus(), port);
        } finally {
            if (nonNull(managedChannel) && !managedChannel.isShutdown()) {
                managedChannel.shutdown();
            }
        }
        return requestVoteReply;
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
        }
    }

    private boolean hasMajorityOfVotes() {
        int majority = parseInt(new DecimalFormat("#").format(((double) (cluster.size() + 1)) / 2));
        LOGGER.debug("I received {} votes and majority is: {}.", receivedVotes + 1, majority);
        return receivedVotes + 1 >= majority;
    }

    private boolean serverStateIs(ServerState... serverState) {
        return stream(serverState).anyMatch(ss -> state == ss);
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
        public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesReply> responseObserver) {
            AppendEntriesReply.Builder builder = AppendEntriesReply.newBuilder();
            final long myTerm;
            LOGGER.debug("Received heartbeat RPC from {}.", request.getLeaderId());
            synchronized (LOCK) {
                myTerm = currentTerm.get();
                if (isHeartBeat(request) && request.getTerm() >= myTerm) {
                    state = FOLLOWER;
                    lastHeartBeatReceived = currentTimeMillis();
                    currentTerm.set(request.getTerm());
                    LOGGER.debug("Received and accepted heartbeat RPC from {}. Current state is: {}",
                            request.getLeaderId(), state.toString());
                }
            }
            builder.setSuccess(request.getTerm() >= myTerm);
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
//                if (request.getLastLogIndex() >= lastLogIndex
//                        && request.getLastLogTerm() >= lastLogTerm) {
                    builder.setVoteGranted(true);
                    votedFor.set(request.getCandidateId());
                    lastHeartBeatReceived = currentTimeMillis();//??????
                    LOGGER.debug("Vote granted to {}", request.getCandidateId());
//                }
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
