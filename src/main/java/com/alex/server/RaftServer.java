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
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alex.raft.RaftServiceGrpc.newBlockingStub;
import static com.alex.server.model.ServerState.*;
import static java.lang.Integer.parseInt;
import static java.lang.System.*;
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
    private Map<String, Integer> nextIndex;
    private Map<String, Integer> matchIndex;

    private static final int ELECTION_TIMER_UPPER_BOUND = 300;
    private static final int ELECTION_TIMER_LOWER_BOUND = 150;
    private final int electionTimeOut;
    private final int leaderHeartbeatTimeout;

    //to be removed
    private long lastHeartBeatReceived;

    private final AtomicBoolean leaderAlreadyElected = new AtomicBoolean(false);

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
        LOGGER.info("Leader heartbeat timeout randomly chosen: {}", leaderHeartbeatTimeout);
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
            if (serverStateIsNot(LEADER)) {
                return;
            }
        }
        for (final Integer server : cluster.keySet()) {
            executorService.execute(() -> {
                long start = currentTimeMillis();
                LOGGER.debug("Sending heartbeat RPC to: {}", server);
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
            if (serverStateIsNot(LEADER)) {
                return;
            }
            leaderAlreadyElected.set(true);
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
            managedChannel = managedChannelMap.computeIfAbsent(port, this::buildChannel);
            blockingStub = stubs.computeIfAbsent(port, integer -> newBlockingStub(managedChannel));
            AppendEntriesReply appendEntriesReply = blockingStub.appendEntries(appendEntriesRequest);
            if (!appendEntriesReply.getSuccess()) {
                LOGGER.debug("Heartbeat to server {} failed.", port);
                checkAndUpdateTerm(appendEntriesReply.getTerm());
            }
        } catch (StatusRuntimeException sre) {
            LOGGER.warn("RPC failed: {}, {}", sre.getStatus(), sre.getMessage());
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
                LOGGER.debug("Started election");
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
            lastHeartBeatReceived = currentTimeMillis();
        }

        for (final Integer server : cluster.keySet()) {
            executorService.execute(() -> {
                synchronized (LOCK) {
                    if (serverStateIsNot(CANDIDATE)) {
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
        final ManagedChannel managedChannel;
        final RaftServiceGrpc.RaftServiceBlockingStub blockingStub;
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
        }
    }

    private boolean hasMajorityOfVotes() {
        int majority = parseInt(new DecimalFormat("#").format(((double) (cluster.size() + 1)) / 2));
        LOGGER.debug("I received {} votes and majority is: {}.", receivedVotes + 1, majority);
        return receivedVotes + 1 >= majority;
    }

    private boolean serverStateIsNot(ServerState... serverState) {
        return stream(serverState).noneMatch(ss -> state == ss);
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
            long start = currentTimeMillis();
            AppendEntriesReply.Builder builder = AppendEntriesReply.newBuilder();
            final long myTerm;
            LOGGER.debug("Received heartbeat RPC from {}.", request.getLeaderId());
            synchronized (LOCK) {
                myTerm = currentTerm.get();
                if (isHeartBeat(request) && request.getTerm() >= myTerm) {
                    state = FOLLOWER;
                    lastHeartBeatReceived = currentTimeMillis();
                    currentTerm.set(request.getTerm());
                    leaderAlreadyElected.set(true);
                    LOGGER.debug("Received and accepted heartbeat RPC from {}. Current state is: {}",
                            request.getLeaderId(), state.toString());
                }
            }
            builder.setSuccess(request.getTerm() >= myTerm);
            builder.setTerm(myTerm);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
            LOGGER.debug("I responded to appendEntriesRPC in: {} ms", currentTimeMillis() - start);
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
                    leaderAlreadyElected.set(true);
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
