package edu.cmu.raftj.persistence;

import com.google.common.collect.ImmutableList;
import edu.cmu.raftj.rpc.Messages.LogEntry;

import javax.annotation.Nullable;
import java.io.Closeable;

/**
 * persistence
 */
public interface Persistence extends Closeable {

    long getCurrentTerm();

    long incrementAndGetCurrentTerm();

    boolean largerThanAndSetCurrentTerm(long term);

    @Nullable
    String getVotedForInCurrentTerm();

    boolean compareAndSetVoteFor(@Nullable String old, @Nullable String vote);

    LogEntry getLogEntry(long index);

    ImmutableList<LogEntry> getLogEntriesFrom(long fromIndex);

    @Nullable
    LogEntry getLastLogEntry();

    void applyLogEntry(LogEntry logEntry);

    long getLastLogIndex();
}
