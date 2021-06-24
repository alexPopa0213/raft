package com.alex.server.util;

import com.alex.server.model.LogEntry;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

public final class Utils {
    private Utils() {
    }

    public static synchronized List<LogEntry> findMissingEntries(List<LogEntry> logEntries, List<com.alex.raft.LogEntry> newEntries) {
        if (newEntries.isEmpty()) {
            return new ArrayList<>();
        }
        if (logEntries.isEmpty()) {
            return newEntries.stream()
                    .map(Utils::fromProto)
                    .collect(toList());
        }
        int firstNewIndex = newEntries.get(0).getIndex();
        int lastIndex = logEntries.get(logEntries.size() - 1).getIndex();
        if (firstNewIndex > lastIndex) {
            return newEntries.stream()
                    .map(Utils::fromProto)
                    .collect(toList());
        }
        return new ArrayList<>();
    }


    public static synchronized List<com.alex.server.model.LogEntry> removeConflictingAndExtraEntries(List<LogEntry> logEntries, List<com.alex.raft.LogEntry> newEntries, int prevLogIndex) {
        if (newEntries.isEmpty()) {
            return removeExtraEntries(logEntries, prevLogIndex);
        }
        for (com.alex.raft.LogEntry entry : newEntries) {
            int idx = entry.getIndex();
            if (idx <= logEntries.size() - 1) {
                com.alex.server.model.LogEntry localEntry = logEntries.get(idx);
                if (localEntry.getTerm() != entry.getTerm()) {
                    return logEntries.subList(0, idx);
                }
            } else {
                break;
            }
        }
        int lastNewIndex = newEntries.get(newEntries.size() - 1).getIndex();
        return removeExtraEntries(logEntries, lastNewIndex);
    }

    private static LogEntry fromProto(com.alex.raft.LogEntry logEntry) {
        return new LogEntry(logEntry.getTerm(), logEntry.getCommand(), logEntry.getIndex());
    }

    public static List<LogEntry> removeExtraEntries(List<LogEntry> logEntries, int upperLimit) {
        if (logEntries.size() - 1 > upperLimit) {
            return logEntries.subList(0, upperLimit + 1);
        }
        return logEntries;
    }
}
