package com.alex.server.mains;

import com.alex.raft.AppendEntriesReply;
import com.alex.server.RaftServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.logging.Logger;

public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws InterruptedException {

        final RaftServer server1 = new RaftServer(50051, "srv1");
        server1.start();

        Thread.sleep(10000);
        for (int port : server1.getCluster().keySet()) {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress("localhost", 50052)
                    .usePlaintext()
                    .build();
            server1.talkTo(channel);
            AppendEntriesReply appendEntriesReply = server1.appendEntries();
            LOGGER.info("received reply:" + appendEntriesReply.toString());
        }

        server1.blockUntilShutdown();
    }
}
