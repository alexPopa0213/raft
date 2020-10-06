package com.alex.server.util;

import com.alex.server.model.LogEntry;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

public final class Utils {
    private Utils() {
    }

    public static synchronized List<LogEntry> findMissingEntries(List<LogEntry> logEntries, List<com.alex.raft.LogEntry> newEntries) {
        for (com.alex.raft.LogEntry logEntry : newEntries) {
            int indexInLog = logEntry.getIndex();
            int indexInList = newEntries.indexOf(logEntry);
            if (logEntries.size() <= indexInLog) {
                return newEntries.subList(indexInList, logEntries.size() - 1)
                        .stream()
                        .map(Utils::fromProto)
                        .collect(toList());
            }
        }
        return new ArrayList<>();
    }


    public static synchronized List<com.alex.server.model.LogEntry> removeConflictingEntries(List<LogEntry> logEntries, List<com.alex.raft.LogEntry> newEntries) {
        for (com.alex.raft.LogEntry entry : newEntries) {
            int idx = entry.getIndex();
            if (idx <= logEntries.size() - 1) {
                com.alex.server.model.LogEntry localEntry = logEntries.get(idx);
                if (localEntry.getTerm() != entry.getTerm()) {
                    return logEntries.subList(0, idx);
                }
            }
        }
        return logEntries;
    }

    private static LogEntry fromProto(com.alex.raft.LogEntry logEntry) {
        return new LogEntry(logEntry.getTerm(), logEntry.getCommand(), logEntry.getIndex());
    }
}
