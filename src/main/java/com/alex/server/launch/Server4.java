package com.alex.server.launch;

import com.alex.server.RaftServer;

public class Server4 {
    public static void main(String[] args) throws InterruptedException {

        final RaftServer server4 = new RaftServer(50054, "srv4");
        server4.start();
        server4.blockUntilShutdown();
    }
}
