package edu.cmu.raftj.persistence;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import edu.cmu.raftj.rpc.Messages.LogEntry;
import edu.cmu.raftj.rpc.Messages.PersistenceEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * file based persistence, append only
 */
public class FilePersistence implements Persistence {

    private static final Logger logger = LoggerFactory.getLogger(FilePersistence.class);

    private final Path path;
    private final OutputStream outputStream;
    private long currentTerm;
    private String voteFor;
    private final List<LogEntry> entries = Lists.newArrayList();
    private final PersistenceEntry.Builder builder = PersistenceEntry.newBuilder();

    public FilePersistence(Path persistencePath) throws IOException {
        this.path = checkNotNull(persistencePath, "persistence path");
        this.currentTerm = 0L;
        this.voteFor = null;

        if (!Files.exists(path)) {
            Files.createFile(path);
            logger.info("{} does not exist, creating for persistence", path);
            outputStream = new FileOutputStream(path.toFile());
        } else {
            recover();
            outputStream = new FileOutputStream(path.toFile(), true);
        }
    }

    private void recover() throws IOException {
        try (InputStream inputStream = new FileInputStream(path.toFile())) {
            int count = 0;
            while (inputStream.available() > 0) {
                PersistenceEntry entry = PersistenceEntry.parseFrom(inputStream);
                logger.debug("recovering entry #{}: {}", count, entry);
                count++;
                switch (entry.getPayloadCase()) {
                    case LOGENTRY:
                        entries.add(entry.getLogEntry());
                        checkState(entries.get(entries.size() - 1).getLogIndex() == entries.size());
                        break;
                    case CURRENTTERM:
                        currentTerm = entry.getCurrentTerm();
                        break;
                    case VOTEDFOR:
                        voteFor = entry.getVotedFor();
                        break;
                    default:
                        throw new IllegalStateException("unknown payload");
                }
            }
            logger.info("recovered {} persistence entries from file {}", count, path);
        }
    }

    @Override
    public synchronized long getCurrentTerm() {
        return currentTerm;
    }

    @Override
    public synchronized long incrementAndGetCurrentTerm() {
        try {
            final long newTerm = currentTerm + 1;
            builder.setCurrentTerm(newTerm).build().writeTo(outputStream);
            currentTerm = newTerm;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return currentTerm;
    }

    @Override
    public synchronized boolean largerThanAndSetCurrentTerm(long term) {
        try {
            if (term > currentTerm) {
                builder.setCurrentTerm(term).build().writeTo(outputStream);
                currentTerm = term;
                return true;
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return false;
    }

    @Nullable
    @Override
    public synchronized String getVotedFor() {
        return voteFor;
    }

    @Override
    public synchronized boolean compareAndSetVoteFor(@Nullable String old, @Nullable String vote) {
        if (Objects.equals(old, voteFor)) {
            try {
                builder.setVotedFor(vote).build().writeTo(outputStream);
                voteFor = vote;
                return true;
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
        return false;
    }

    @Override
    public synchronized LogEntry getLogEntry(long index) {
        // we have to be practical
        return entries.get(Ints.checkedCast(index));
    }

    @Nullable
    @Override
    public synchronized LogEntry getLastLogEntry() {
        if (entries.isEmpty()) {
            return null;
        }
        return entries.get(entries.size() - 1);
    }

    @Override
    public synchronized long getLogEntriesSize() {
        return entries.size();
    }

    @Override
    public synchronized void appendLogEntry(LogEntry logEntry) {
        try {
            builder.setLogEntry(checkNotNull(logEntry, "log entry")).build().writeTo(outputStream);
            entries.add(logEntry);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        outputStream.close();
    }
}
