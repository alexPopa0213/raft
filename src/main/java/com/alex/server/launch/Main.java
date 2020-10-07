package com.alex.server.launch;

import com.alex.server.RaftServer;
import com.alex.server.model.LogEntry;
import com.alex.server.model.LogEntrySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.IndexTreeList;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws InterruptedException {
        final RaftServer server1 = new RaftServer(50051, "srv1");
        server1.start();
        server1.blockUntilShutdown();

//        String dbName = "db_srv1";
//        DB db = DBMaker.fileDB(dbName).checksumHeaderBypass().closeOnJvmShutdown().make();
//        Atomic.Integer test_prevLogIndex = db.atomicInteger("test2_prevLogIndex").createOrOpen();
//
//        System.out.println("Value is: " + test_prevLogIndex.get());

//        List<String> serversToClean = Arrays.asList("srv1", "srv2");
//        for (String server : serversToClean) {
//            String dbName = "db_" + server;
//            DB db = DBMaker.fileDB(dbName).checksumHeaderBypass().closeOnJvmShutdown().make();
//            IndexTreeList<LogEntry> logEntries = db.indexTreeList(server + "_log", new LogEntrySerializer()).createOrOpen();
//            logEntries.clear();
//        }

//        final ExecutorService executorService = newCachedThreadPool();
//        Timer timer = new Timer(true);
//        Map<Integer, ManagedChannel> channels = new ConcurrentHashMap<>();
//
//        List<Integer> ports = Arrays.asList(50052, 50053, 50054, 50055, 50056, 50057);
//        ports.forEach(port -> {
//            ManagedChannel managedChannel = ManagedChannelBuilder.forAddress("127.0.0.1", port)
//                    .executor(executorService)
//                    .usePlaintext()
//                    .build();
//            channels.put(port, managedChannel);
//        });
//
//        timer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                for (Integer port : ports) {
//                    executorService.execute(() -> {
//                        long start = System.currentTimeMillis();
//
//                        AppendEntriesRequest appendEntriesRequest = AppendEntriesRequest.newBuilder()
//                                .setLeaderId("srv1")
//                                .setTerm(1)
//                                .setLeaderCommitIndex(1)
//                                .setPrevLogIndex(0L)
//                                .setPrevLogTerm(0L)
//                                .addAllEntries(new ArrayList<>())
//                                .build();
//                        RaftServiceGrpc.RaftServiceBlockingStub stub = RaftServiceGrpc.newBlockingStub(channels.get(port));
//                        AppendEntriesReply reply = stub.appendEntries(appendEntriesRequest);
//
//                        System.out.println("Append entries to port " + port + " took: "
//                                + (System.currentTimeMillis() - start) + " ms");
//                    });
//                }
//            }
//        }, 0, 50);
//
//        server1.blockUntilShutdown();
    }
}
