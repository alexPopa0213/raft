package com.alex.server.mains;

import com.alex.server.RaftServer;

public class Server6 {
    public static void main(String[] args) throws InterruptedException {

        final RaftServer server6 = new RaftServer(50056, "srv6");
        server6.start();
        server6.blockUntilShutdown();
    }
}
