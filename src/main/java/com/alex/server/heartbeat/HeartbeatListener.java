package com.alex.server.heartbeat;

import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

import static java.lang.Integer.parseInt;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.logging.log4j.LogManager.getLogger;

public class HeartbeatListener implements Runnable {
    private static final Logger LOGGER = getLogger(HeartbeatListener.class);

    private MulticastSocket socket = null;
    private final byte[] buf = new byte[256];

    private final Map<Integer, Timestamp> cluster;
    private final int port;

    public HeartbeatListener(Map<Integer, Timestamp> cluster, int port) {
        this.cluster = cluster;
        this.port = port;
    }

    @Override
    public void run() {
        while (true) {
            try {
                initSocket();
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                int receivedPort = parseInt(new String(packet.getData(), UTF_8).trim());
                if (receivedPort != port) {
                    cluster.put(receivedPort, new Timestamp(new Date().getTime()));
                    LOGGER.trace("Server: {} joined", receivedPort);
                }
            } catch (IOException ex) {
                LOGGER.error(ex.getMessage(), ex);
            }
        }
    }

    private void initSocket() {
        try {
            socket = new MulticastSocket(4466);
            InetAddress group = InetAddress.getByName("230.0.0.0");
            socket.joinGroup(group);
        } catch (IOException ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }

}
