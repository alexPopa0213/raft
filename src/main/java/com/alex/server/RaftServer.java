package com.alex.server;

import com.alex.raft.*;
import com.alex.server.heartbeat.HeartbeatClusterRefresher;
import com.alex.server.heartbeat.HeartbeatListener;
import com.alex.server.heartbeat.HeartbeatPublisher;
import com.alex.server.model.LogEntry;
import com.alex.server.model.LogEntrySerializer;
import com.alex.server.model.ServerState;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.Logger;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import static com.alex.raft.RaftServiceGrpc.newBlockingStub;
import static com.alex.server.model.ServerState.*;
import static io.grpc.Status.UNAVAILABLE;
import static java.lang.Integer.parseInt;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.err;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.logging.log4j.LogManager.getLogger;

public class RaftServer implements Identifiable {
    private static final Logger LOGGER = getLogger(RaftServer.class);

    private ServerState state;

    private final int port;
    private final String id;
    private Server server;

    private final Map<Integer, Timestamp> cluster = new ConcurrentHashMap<>();
//    private final Map<Integer, RaftServiceGrpc.RaftServiceBlockingStub> stubs = new ConcurrentHashMap<>();

    private final Map<Integer, ManagedChannel> managedChannelMap = new ConcurrentHashMap<>();

    private DB db;

    /*
    state persisted to mapDB
     */
    private long currentTerm;
    private String votedFor;
    private List<LogEntry> log;

    /*
    volatile state
     */
    private int commitIndex;
    private int lastApplied;

    private int receivedVotes;

    /*
    leader specific volatile state
     */
    private Map<String, Integer> nextIndex;
    private Map<String, Integer> matchIndex;

    private static final int ELECTION_TIMER_UPPER_BOUND = 1000;
    private static final int ELECTION_TIMER_LOWER_BOUND = 350;
    private final int electionTimeOut;

    private long lastHeartBeatReceived;

//    private RaftServiceGrpc.RaftServiceBlockingStub stub;

    public RaftServer(int port, String id) {
        this.port = port;
        this.id = id;
        initDb(id);
        initVolatileState();
        state = FOLLOWER;
        electionTimeOut = generateRandomElectionTimeout();
    }

    private int generateRandomElectionTimeout() {
        return new Random().nextInt(ELECTION_TIMER_UPPER_BOUND - ELECTION_TIMER_LOWER_BOUND) + ELECTION_TIMER_LOWER_BOUND;
    }

    private void initVolatileState() {
        commitIndex = 0;
        lastApplied = 0;
        receivedVotes = 0;
    }

    private void initDb(String id) {
        String dbName = "db_" + id;
        db = DBMaker.fileDB(dbName).transactionEnable().closeOnJvmShutdown().make();
        log = db.indexTreeList(id + "_log", new LogEntrySerializer()).createOrOpen();
        votedFor = db.atomicString(id + "_votedFor").createOrOpen().get();
        currentTerm = db.atomicLong(id + "_current_term").createOrOpen().get();
    }

//    public void talkTo(Channel channel) {
//        stub = newBlockingStub(channel);
//    }

    public void sendEmptyAppendEntriesRPC(int port) {
        AppendEntriesRequest appendEntriesRequest = AppendEntriesRequest.newBuilder()
                .setLeaderId(id)
                .setTerm(currentTerm)
                .setLeaderCommitIndex(commitIndex)
                .setPrevLogIndex(0L)
                .setPrevLogTerm(0L)
                .addAllEntries(new ArrayList<>())
                .build();
        ManagedChannel managedChannel = null;
        try {
//            stubs.putIfAbsent(port, newBlockingStub(buildChannel(port)));
//            stubs.get(port).appendEntries(appendEntriesRequest);
            managedChannel = ManagedChannelBuilder.forAddress("localhost", port)
                    .usePlaintext()
                    .build();
            managedChannelMap.putIfAbsent(port, managedChannel);
            RaftServiceGrpc.RaftServiceBlockingStub blockingStub = newBlockingStub(managedChannel);
            blockingStub.appendEntries(appendEntriesRequest);
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

    private ManagedChannel buildChannel(Integer port) {
        return ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext()
                .build();
    }

    public RequestVoteReply requestVote(Integer port) {
        int lastLogIndex = log.size() - 1;
        RequestVoteRequest requestVoteRequest = RequestVoteRequest.newBuilder()
                .setTerm(currentTerm)
                .setCandidateId(id)
                .setLastLogIndex(lastLogIndex)
                .setLastLogTerm(lastLogIndex != -1 ? log.get(lastLogIndex).getTerm() : -1)
                .build();
        RequestVoteReply requestVoteReply = null;
        ManagedChannel managedChannel = null;
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

    private void removeServerIfUnavailable(Status status, Integer port) {
        if (status == UNAVAILABLE) { //maybe retry first?
            cluster.remove(port);
//            stubs.remove(port);
        }
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
        enableHeartbeat();
        preventElectionss();
        handleElections();
        addShutdownHook();
    }

    private void enableHeartbeat() {
        listenHeartbeats();
        sendHeartbeats();
        refreshCluster();
    }

    private void preventElectionss() {
        Thread preventElectionsThread = new Thread(() -> {
            while (state == LEADER) {
                preventElections();
            }
        });
        ScheduledExecutorService executor = newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(preventElectionsThread, 0, 200, MILLISECONDS);
    }

    private void listenHeartbeats() {
        new Thread(new HeartbeatListener(cluster, port)).start();
//        ScheduledExecutorService executor = newScheduledThreadPool(1);
//        executor.scheduleAtFixedRate(new HeartbeatListener(cluster, port), 0, 100, MILLISECONDS);
    }

    private void sendHeartbeats() {
        ScheduledExecutorService executor = newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(new HeartbeatPublisher(port), 0, 200, MILLISECONDS);
    }

    private void refreshCluster() {
        ScheduledExecutorService executor = newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(new HeartbeatClusterRefresher(cluster), 0, 200, MILLISECONDS);
    }

    private void handleElections() {
        ScheduledExecutorService executor = newScheduledThreadPool(1);
        Runnable command = () -> {
            if ((state == FOLLOWER || state == CANDIDATE) && lastHeartBeatReceived + electionTimeOut < currentTimeMillis()) {
                startElection();
            }
//            else if (state == LEADER) {
//                preventElections();
//            }
        };
        executor.scheduleAtFixedRate(command, 0, electionTimeOut, MILLISECONDS);
    }

    private void preventElections() {
        cluster.keySet().parallelStream().forEach(port -> {
            LOGGER.debug("Sending empty AppendEntriesRPC (to prevent election) to: {}", port);
            sendEmptyAppendEntriesRPC(port);
        });
    }

    private void startElection() {
        LOGGER.debug("Started election.");
        convertToCandidate();
        currentTerm++;
        votedFor = null;

        cluster.keySet().parallelStream().forEach(port -> {
            if (state == CANDIDATE) {
                LOGGER.debug("Sending RequestVoteRpc to:" + port);
                RequestVoteReply requestVoteReply = requestVote(port);
                if (nonNull(requestVoteReply) && requestVoteReply.getVoteGranted()) {
                    receivedVotes++;
                    if (hasMajorityOfVotes()) {
                        LOGGER.debug("Server : {}, Switched to LEADER state.", id);
                        synchronized (this) {
                            state = LEADER;
                        }
                    }
                }
            }
        });
    }

    private void convertToCandidate() {
        synchronized (this) {
            state = CANDIDATE;
        }
    }

    private boolean hasMajorityOfVotes() {
        return receivedVotes + 1 >= parseInt(new DecimalFormat("#").format(((double) (cluster.size() + 1)) / 2));
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
            builder.setSuccess(currentTerm <= request.getTerm());
            if (isHeartBeat(request)) {
                state = FOLLOWER;
                lastHeartBeatReceived = currentTimeMillis();
                LOGGER.debug("Received heartbeat RPC from {}. Current state is: {}",
                        request.getLeaderId(), state.toString());
            }
            builder.setTerm(currentTerm);
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
            builder.setTerm(currentTerm);
            int lastLogIndex = log.size() - 1;
            long lastLogTerm = lastLogIndex != -1 ? log.get(lastLogIndex).getTerm() : -1;
            if (request.getTerm() < currentTerm) {
                builder.setVoteGranted(false);
            } else if (isNull(votedFor)
                    && request.getLastLogIndex() >= lastLogIndex
                    && request.getLastLogTerm() >= lastLogTerm) {
                builder.setVoteGranted(true);
                votedFor = request.getCandidateId();
            }
//            builder.setVoteGranted(request.getCandidateId().equals("srv1"));
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }
    }
}
