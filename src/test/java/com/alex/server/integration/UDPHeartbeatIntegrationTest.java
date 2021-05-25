package com.alex.server.integration;

import com.alex.server.heartbeat.udp.HeartbeatClusterRefresherTimerTask;
import com.alex.server.heartbeat.udp.HeartbeatListenerTimerTask;
import com.alex.server.heartbeat.udp.HeartbeatPublisherTimerTask;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UDPHeartbeatIntegrationTest {

    private static final String address1 = "40001";
    private static final String address2 = "40002";
    private static final String address3 = "40003";
    private static final String address4 = "40004";

    private final Map<String, Timestamp> cluster = new ConcurrentHashMap<>();

    @Before
    public void prepare() {
        cluster.clear();
    }

    @Test
    public void test_multicast_heartbeat() {
        HeartbeatPublisherTimerTask heartbeatPublisherTimerTask1 = new HeartbeatPublisherTimerTask(address2);
        HeartbeatPublisherTimerTask heartbeatPublisherTimerTask2 = new HeartbeatPublisherTimerTask(address3);
        HeartbeatPublisherTimerTask heartbeatPublisherTimerTask3 = new HeartbeatPublisherTimerTask(address4);

        HeartbeatListenerTimerTask heartbeatListenerTimerTask1 = new HeartbeatListenerTimerTask(cluster, address1);

        while (cluster.size() < 3) {
            heartbeatListenerTimerTask1.run();
            new Thread(heartbeatPublisherTimerTask1).start();
            new Thread(heartbeatPublisherTimerTask2).start();
            new Thread(heartbeatPublisherTimerTask3).start();
        }

        assertEquals(3, cluster.keySet().size());
        assertTrue(cluster.keySet().containsAll(asList(address4, address3, address2)));
    }

    @Test
    public void test_old_servers_are_removed() {
        HeartbeatClusterRefresherTimerTask heartbeatClusterRefresherTimerTask = new HeartbeatClusterRefresherTimerTask(cluster);
    }
}
