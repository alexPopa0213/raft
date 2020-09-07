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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.alex.raft.RaftServiceGrpc.newBlockingStub;
import static com.alex.server.model.ServerState.*;
import static java.lang.Integer.parseInt;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.err;
import static java.util.Arrays.stream;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.logging.log4j.LogManager.getLogger;

public class RaftServer implements Identifiable {
    private static final Logger LOGGER = getLogger(RaftServer.class);

    private volatile ServerState state;

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
    private volatile Atomic.Long currentTerm;
    private volatile Atomic.String votedFor;
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

    private static final int ELECTION_TIMER_UPPER_BOUND = 3500;
    private static final int ELECTION_TIMER_LOWER_BOUND = 2000;
    private final int electionTimeOut;

    private volatile long lastHeartBeatReceived;

    private final ExecutorService executorService = newCachedThreadPool();
    private final Timer timerQueue = new Timer(true);
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
        enableHeartbeat();
        preventElections2();
        handleElections2();
        addShutdownHook();
    }

    private void enableHeartbeat() {
        listenHeartbeats();
        sendHeartbeats();
        refreshCluster();
    }

    private void preventElections() {
        Thread preventElectionsThread = new Thread(() -> {
            if (serverStateIs(LEADER)) {
                sendHeartbeatRPC();
            }
//            LOGGER.debug("Prevent elections thread stopped!");
        });
//        preventElectionsThread.start();
        ScheduledExecutorService executor = newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(preventElectionsThread, 0, 150, MILLISECONDS);
    }

    private void preventElections2() {
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                if (serverStateIs(LEADER)) {
                    sendHeartbeatRPC2();
                }
            }
        };
        timerQueue.schedule(timerTask, 0, 200);
    }

    private void handleElections() {
        ScheduledExecutorService executor = newScheduledThreadPool(1);
        Runnable command = () -> {
            long currentTimeMillis = currentTimeMillis();
            if (serverStateIs(FOLLOWER, CANDIDATE)
                    && lastHeartBeatReceived + electionTimeOut < currentTimeMillis) {
                LOGGER.debug("Didn't receive heartbeat for: {}", currentTimeMillis - lastHeartBeatReceived - electionTimeOut);
                startElection();
            }
//            else if (state == LEADER) {
//                preventElections();
//            }
        };
        executor.scheduleAtFixedRate(command, 0, electionTimeOut, MILLISECONDS);
    }

    private void handleElections2() {
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                long currentTimeMillis = currentTimeMillis();
                if (serverStateIs(FOLLOWER, CANDIDATE)
                        && lastHeartBeatReceived + electionTimeOut < currentTimeMillis) {
                    LOGGER.debug("Didn't receive heartbeat for: {}", currentTimeMillis - lastHeartBeatReceived - electionTimeOut);
                    startElection2();
                }
            }
        };
        timerQueue.schedule(timerTask, 0, electionTimeOut);
    }

    public void sendEmptyAppendEntriesRPC(int port) {
        AppendEntriesRequest appendEntriesRequest = AppendEntriesRequest.newBuilder()
                .setLeaderId(id)
                .setTerm(currentTerm.get())
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
            AppendEntriesReply appendEntriesReply = blockingStub.appendEntries(appendEntriesRequest);
            if (!appendEntriesReply.getSuccess()) {
                updateTerm(appendEntriesReply.getTerm());
                convertTo(FOLLOWER);
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

    private ManagedChannel buildChannel(Integer port) {
        return ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext()
                .build();
    }

    public RequestVoteReply requestVote(Integer port) {
        int lastLogIndex = log.size() - 1;
        RequestVoteRequest requestVoteRequest = RequestVoteRequest.newBuilder()
                .setTerm(currentTerm.get())
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
        if (status.getCode().equals(Status.Code.UNAVAILABLE)) { //maybe retry first?
            cluster.remove(port);
        }
    }

    private void listenHeartbeats() {
        new Thread(new HeartbeatListener(cluster, port)).start();
    }

    private void sendHeartbeats() {
//        ScheduledExecutorService executor = newScheduledThreadPool(1);
//        executor.scheduleAtFixedRate(new HeartbeatPublisher(port), 0, 200, MILLISECONDS);
        timerQueue.schedule(new HeartbeatPublisherTimerTask(port), 0, 200);
    }

    private void refreshCluster() {
//        ScheduledExecutorService executor = newScheduledThreadPool(1);
//        executor.scheduleAtFixedRate(new HeartbeatClusterRefresher(cluster), 0, 200, MILLISECONDS);
        timerQueue.schedule(new HeartbeatClusterRefresherTimerTask(cluster), 0, 500);
    }

    private void sendHeartbeatRPC() {
        long start = currentTimeMillis();
        cluster.keySet().parallelStream().forEach(port -> {
            LOGGER.trace("Sending empty AppendEntriesRPC (to prevent election) to: {}", port);
            sendEmptyAppendEntriesRPC(port);
        });
        long end = currentTimeMillis();
        LOGGER.debug("Sending HeartbeatRPC to all servers took: {} milliseconds.", end - start);
    }

    private void sendHeartbeatRPC2() {
        for (final Integer server : cluster.keySet()) {
            executorService.execute(() -> {
                if (serverStateIs(LEADER)) {
                    sendEmptyAppendEntriesRPC(server);
                }
            });
        }
    }

    private void startElection() {
        LOGGER.debug("Started election.");
        convertTo(CANDIDATE);
        updateTerm(currentTerm.get() + 1);
        votedFor.set(null);

        cluster.keySet().parallelStream().forEach(port -> {
            if (serverStateIs(CANDIDATE)) {
                LOGGER.debug("Sending RequestVoteRpc to:" + port);
                RequestVoteReply requestVoteReply = requestVote(port);
                if (nonNull(requestVoteReply)) {
                    if (requestVoteReply.getVoteGranted()) {
                        LOGGER.debug("Received voted from: {}", port);
                        receivedVotes++;
                        if (hasMajorityOfVotes()) {
                            LOGGER.debug("Server : {}, Switched to LEADER state.", id);
                            convertTo(LEADER);
                            receivedVotes = 0;
//                            preventElections();
                        }
                    } else {
                        updateTerm(requestVoteReply.getTerm());
                        convertTo(FOLLOWER);
                    }
                }
            }
        });
    }

    private void startElection2() {
        LOGGER.debug("Started election.");
        convertTo(CANDIDATE);
        updateTerm(currentTerm.get() + 1);
        votedFor.set(null);

        for (final Integer server : cluster.keySet()) {
            executorService.execute(() -> {
                if (serverStateIs(CANDIDATE)) {
                    LOGGER.debug("Sending RequestVoteRpc to:" + server);
                    RequestVoteReply requestVoteReply = requestVote(server);
                    if (nonNull(requestVoteReply)) {
                        if (requestVoteReply.getVoteGranted()) {
                            LOGGER.debug("Received voted from: {}", server);
                            receivedVotes++;
                            if (hasMajorityOfVotes()) {
                                LOGGER.debug("Server : {}, Switched to LEADER state.", id);
                                convertTo(LEADER);
                                receivedVotes = 0;
                            }
                        } else {
                            updateTerm(requestVoteReply.getTerm());
                            convertTo(FOLLOWER);
                        }
                    }
                }
            });
        }
    }

    private boolean hasMajorityOfVotes() {
        int majority = parseInt(new DecimalFormat("#").format(((double) (cluster.size() + 1)) / 2));
        LOGGER.debug("I received {} votes out of {}.", receivedVotes + 1, majority);
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


    //    private void incrementCurrentTerm() {
//        readWriteLock.writeLock().lock();
//        currentTerm.set(currentTerm() + 1);
//        readWriteLock.writeLock().unlock();
//    }
    private boolean serverStateIs(ServerState... serverState) {
//        readWriteLock.readLock().lock();
        boolean result = stream(serverState).anyMatch(ss -> state == ss);
//        readWriteLock.readLock().unlock();
        return result;
    }

    private void convertTo(ServerState serverState) {
//        readWriteLock.writeLock().lock();
//        readWriteLock.writeLock().unlock();
        if (serverState != state) {
            LOGGER.debug("Old state is: {}.", state.toString());
            state = serverState;
            LOGGER.debug("Updated state is: {} and term is {}", state.toString(), currentTerm.get());
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

//    private Long currentTerm() {
//        readWriteLock.readLock().lock();
//        Long term = currentTerm.get();
//        readWriteLock.readLock().unlock();
//        return term;
//    }

//    private String votedFor() {
//        readWriteLock.readLock().lock();
//        String serverId = votedFor.get();
//        readWriteLock.readLock().unlock();
//        return serverId;
//    }

    @Override
    public String getId() {
        return id;
    }

    class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
        @Override
        public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesReply> responseObserver) {
            AppendEntriesReply.Builder builder = AppendEntriesReply.newBuilder();
            long term = currentTerm.get();
            if (isHeartBeat(request) && request.getTerm() >= term) {
                convertTo(FOLLOWER);
                lastHeartBeatReceived = currentTimeMillis();
                updateTerm(request.getTerm());
                LOGGER.debug("Received and accepted heartbeat RPC from {}. Current state is: {}",
                        request.getLeaderId(), state.toString());
            }
            builder.setSuccess(request.getTerm() >= term);
            builder.setTerm(term);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        private boolean isHeartBeat(AppendEntriesRequest request) {
            return request.getEntriesCount() == 0;
        }

        @Override
        public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteReply> responseObserver) {
            LOGGER.debug("Received RequestVoteRPC from: {}", request.getCandidateId());
            long term = currentTerm.get();
            LOGGER.debug("Current term is: {} and received term is: {}", term, request.getTerm());
            RequestVoteReply.Builder builder = RequestVoteReply.newBuilder();
            builder.setTerm(term);
            int lastLogIndex = log.size() - 1;
            long lastLogTerm = lastLogIndex != -1 ? log.get(lastLogIndex).getTerm() : -1;
            if (request.getTerm() <= term) {
                builder.setVoteGranted(false);
                builder.setTerm(term);
                LOGGER.debug("Vote NOT granted to {} because term was lower (or not bigger).", request.getCandidateId());
            } else if (isNull(votedFor.get())) {
//                if (request.getLastLogIndex() >= lastLogIndex
//                        && request.getLastLogTerm() >= lastLogTerm) {
                builder.setVoteGranted(true);
                votedFor.set(request.getCandidateId());
                LOGGER.debug("Vote granted to {}", request.getCandidateId());
//                }
            } else {
                LOGGER.debug("Vote NOT granted to {} because I already voted for {}.", request.getCandidateId(), votedFor.get());
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }
    }

    private void updateTerm(long newTerm) {
        if (newTerm > currentTerm.get()) {
            LOGGER.debug("Old term is: {}", currentTerm.get());
            currentTerm.set(newTerm);
            LOGGER.debug("Updated term is: {}", currentTerm.get());
        }
    }

}
