package com.alex.server.launch;

import com.alex.server.RaftServer;

public class Server3 {
    public static void main(String[] args) throws InterruptedException {

        final RaftServer server3 = new RaftServer(50053, "srv3");
        server3.start();
        server3.blockUntilShutdown();
    }
}