package com.alex.server.launch;

import com.alex.server.RaftServer;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        final RaftServer server1 = new RaftServer(50051, "srv1");
        server1.start();
        server1.blockUntilShutdown();
    }
}
