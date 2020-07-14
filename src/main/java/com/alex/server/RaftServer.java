package com.alex.server;

import com.alex.raft.AppendEntriesReply;
import com.alex.raft.AppendEntriesRequest;
import com.alex.raft.RaftServiceGrpc;
import com.alex.server.heartbeat.HeartbeatClusterRefresher;
import com.alex.server.heartbeat.HeartbeatListener;
import com.alex.server.heartbeat.HeartbeatPublisher;
import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.System.err;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.apache.logging.log4j.LogManager.getLogger;

public class RaftServer implements Identifiable {
    private static final Logger LOGGER = getLogger(RaftServer.class);

    private final int port;
    private final String id;
    private Server server;

    private final Map<Integer, Timestamp> cluster;

    private RaftServiceGrpc.RaftServiceBlockingStub stub;

    public RaftServer(int port, String id) {
        this.port = port;
        this.id = id;
        cluster = new ConcurrentHashMap<>();
    }

    public void talkTo(Channel channel) {
        stub = RaftServiceGrpc.newBlockingStub(channel);
    }

    public AppendEntriesReply appendEntries() {
        AppendEntriesRequest appendEntriesRequest = AppendEntriesRequest.newBuilder()
                .setLeaderId("srv1")
                .setTerm(1)
                .build();
        AppendEntriesReply appendEntriesReply = null;
        try {
            appendEntriesReply = stub.appendEntries(appendEntriesRequest);
        } catch (StatusRuntimeException sre) {
            LOGGER.warn("RPC failed: {}, {}", sre.getStatus(), sre.getMessage());
        }
        return appendEntriesReply;
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
        ScheduledExecutorService executor = newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(new HeartbeatPublisher(port), 0, 3, TimeUnit.SECONDS);
    }

    private void refreshCluster() {
        ScheduledExecutorService executor = newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(new HeartbeatClusterRefresher(cluster), 0, 5, TimeUnit.SECONDS);
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
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

    public Map<Integer, Timestamp> getCluster() {
        return cluster;
    }

    static class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
        @Override
        public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesReply> responseObserver) {
            AppendEntriesReply appendEntriesReply = AppendEntriesReply.newBuilder().setSuccess(true).setTerm(1L).build();
            responseObserver.onNext(appendEntriesReply);
            responseObserver.onCompleted();
        }

    }
}
