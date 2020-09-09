package com.alex.server.mains;

import com.alex.server.RaftServer;

public class Server2 {
    public static void main(String[] args) throws InterruptedException {

        final RaftServer server2 = new RaftServer(50052, "srv2");
        server2.start();
        server2.blockUntilShutdown();
    }
}
