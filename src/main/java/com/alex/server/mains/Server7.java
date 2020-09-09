package com.alex.server.mains;

import com.alex.server.RaftServer;

public class Server7 {
    public static void main(String[] args) throws InterruptedException {

        final RaftServer server7 = new RaftServer(50057, "srv7");
        server7.start();
        server7.blockUntilShutdown();
    }
}
