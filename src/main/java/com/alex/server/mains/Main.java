package com.alex.server.mains;

import com.alex.server.RaftServer;

import java.util.logging.Logger;

public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws InterruptedException {
        final RaftServer server1 = new RaftServer(50051, "srv1");
        server1.start();
        server1.blockUntilShutdown();

//        Thread.sleep(5000);
//
//        ManagedChannel channel = ManagedChannelBuilder
//                .forAddress("localhost", 50052)
//                .usePlaintext()
//                .build();
//        server1.talkTo(channel);
//        server1.sendEmptyAppendEntriesRPC();


//        DB db = server1.getDb();
//        HTreeMap<String, String> map = db.hashMap("mapdbtest")
//                .keySerializer(Serializer.STRING)
//                .valueSerializer(Serializer.STRING)
//                .createOrOpen();

//        map.put("key1", "value1");
//        Atomic.String atomic = db.atomicString("atomic").createOrOpen();
//        atomic.set("plm");

//        System.out.println(atomic.get());

//        map.close();

    }
}
