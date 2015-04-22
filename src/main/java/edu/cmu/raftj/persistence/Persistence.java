package edu.cmu.raftj.persistence;

import edu.cmu.raftj.rpc.Messages.LogEntry;

import javax.annotation.Nullable;
import java.io.Closeable;

/**
 * persistence
 */
public interface Persistence extends Closeable {

    long getCurrentTerm();

    long incrementAndGetCurrentTerm();

    @Nullable
    String getVotedFor();

    boolean compareAndSetVoteFor(@Nullable String old, @Nullable String vote);

    LogEntry getLogEntry(int index);

    void appendLogEntry(LogEntry logEntry);

    int getLogEntrySize();
}
