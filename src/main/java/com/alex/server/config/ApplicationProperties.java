package com.alex.server.config;

public final class ApplicationProperties {

    private ApplicationProperties() {
    }

    public static final String VOID_VALUE = "";

    public static final int ELECTION_TIMER_UPPER_BOUND = 350;
    public static final int ELECTION_TIMER_LOWER_BOUND = 250;
    public static final int LEADER_HEARTBEAT_SPLIT = 4;

    // units in ms
    public static final int LISTEN_UDP_HEARTBEATS_INTERVAL = 50;
    public static final int SEND_UDP_HEARTBEATS_INTERVAL = 300;
    public static final int UDP_REFRESH_CLUSTER_INTERVAL = 1000;

    // units in seconds
    public static final int UDP_HEARTBEAT_EXPIRATION_TIME = 5;

    public static final int minimumClusterSize = 3;

    // persisted state
    public static final String DB_NAME_PREFIX = "db_";
    public static final String LOG_NAME_SUFFIX = "_log";
    public static final String VOTED_FOR_SUFFIX = "_votedFor";
    public static final String CURRENT_TERM_SUFFIX = "_current_term";

}
