package com.alex.server.model;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;

public class LogEntrySerializer implements Serializer<LogEntry> {
    @Override
    public void serialize(@NotNull DataOutput2 out, @NotNull LogEntry value) throws IOException {
        out.writeLong(value.getTerm());
        out.writeChars(value.getCommand());
        out.writeInt(value.getIndex());
    }

    @Override
    public LogEntry deserialize(@NotNull DataInput2 input, int available) throws IOException {
        return new LogEntry(input.readLong(), input.readLine(), input.readInt());
    }

}
