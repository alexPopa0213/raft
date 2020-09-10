package com.alex.server.launch;

import com.alex.server.RaftServer;

public class Server5 {
    public static void main(String[] args) throws InterruptedException {

        final RaftServer server5 = new RaftServer(50055, "srv5");
        server5.start();
        server5.blockUntilShutdown();
    }
}
