package edu.cmu.raftj.server;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import edu.cmu.raftj.persistence.Persistence;
import edu.cmu.raftj.rpc.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * logging state machine
 */
public class LoggingStateMachine implements StateMachine {

    private static final Logger logger = LoggerFactory.getLogger(LoggingStateMachine.class);

    private final AtomicLong commitIndex = new AtomicLong(0);
    private final AtomicLong lastApplied = new AtomicLong(0);
    private final ExecutorService stateMachineThread = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("StateMachine").build());

    @Override
    public long getCommitIndex() {
        return commitIndex.get();
    }

    @Override
    public void increaseCommitIndex(long possibleLargerCommitIndex) {
        commitIndex.updateAndGet(value -> Math.max(value, possibleLargerCommitIndex));
    }

    @Override
    public void applyAllPendingCommandsFrom(final Persistence persistence) {
        try {
            stateMachineThread.submit(() -> {
                while (lastApplied.get() < commitIndex.get()) {
                    long index = lastApplied.getAndIncrement();
                    Messages.LogEntry logEntry = checkNotNull(persistence.getLogEntry(index), "index %s is null", index);
                    String command = logEntry.getCommand();
                    applyCommand(index, command);
                }
            }).get();
        } catch (InterruptedException | ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * apply command to the state machine
     *
     * @param command command
     */
    private void applyCommand(long index, String command) {
        logger.info("applying command #{}: '{}'", index, command);
    }
}
