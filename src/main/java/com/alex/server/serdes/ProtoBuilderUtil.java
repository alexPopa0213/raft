package com.alex.server.serdes;

import com.alex.raft.LogEntry;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class ProtoBuilderUtil {

    public static List<LogEntry> toProto(List<com.alex.server.model.LogEntry> entries) {
        return entries.stream()
                .map(ProtoBuilderUtil::buildProtoLogEntry)
                .collect(toList());
    }

    public static com.alex.raft.LogEntry buildProtoLogEntry(com.alex.server.model.LogEntry entry) {
        return com.alex.raft.LogEntry.newBuilder()
                .setTerm(entry.getTerm())
                .setCommand(entry.getCommand())
                .setIndex(entry.getIndex())
                .build();
    }
}
