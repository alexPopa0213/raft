package com.alex.server.heartbeat.udp;

import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.TimerTask;

import static java.lang.Integer.parseInt;
import static java.lang.Runtime.getRuntime;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.logging.log4j.LogManager.getLogger;

public class HeartbeatListenerTimerTask extends TimerTask {
    private static final Logger LOGGER = getLogger(HeartbeatListenerTimerTask.class);

    private MulticastSocket socket = null;
    private final byte[] buf = new byte[256];

    private final Map<Integer, Timestamp> cluster;
    private final int port;
    private final int socketTimeout;

    public HeartbeatListenerTimerTask(Map<Integer, Timestamp> cluster, int port) {
        this.cluster = cluster;
        this.port = port;
        socketTimeout = 200;
        initSocket();
        addShutdownHook();
    }

    @Override
    public void run() {
        try {
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            socket.setSoTimeout(socketTimeout);
            socket.receive(packet);
            int receivedPort = parseInt(new String(packet.getData(), UTF_8).trim());
            if (receivedPort != port) {
                if (!cluster.containsKey(receivedPort)) {
                    LOGGER.debug("Server: {} joined", receivedPort);
                }
                cluster.put(receivedPort, new Timestamp(new Date().getTime()));
            }
        } catch (SocketTimeoutException ste) {
            LOGGER.trace("No packet received in the last {} ms,", socketTimeout);
        } catch (IOException ex) {
            LOGGER.error(ex.getMessage(), ex);
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

    private void addShutdownHook() {
        getRuntime().addShutdownHook(new Thread(() -> {
            socket.close();
        }));
    }
}
