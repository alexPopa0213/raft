package com.alex.server.mains;

import com.alex.server.RaftServer;

public class Main3 {
    public static void main(String[] args) throws InterruptedException {

        final RaftServer server3 = new RaftServer(50053, "srv3");
        server3.start();
        server3.blockUntilShutdown();
    }
}
