package com.alex.server.model;

import org.mapdb.Atomic;

import java.util.List;

public class RaftPersistentState {

    private Atomic.Long currentTerm;
    private Atomic.String votedFor;
    private List<LogEntry> log;


}
