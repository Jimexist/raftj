package edu.cmu.raftj.persistence;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import edu.cmu.raftj.rpc.Messages.DiskPersistenceEntry;
import edu.cmu.raftj.rpc.Messages.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * disk persistence
 */
public class DiskPersistence implements Persistence {

    private static final Logger logger = LoggerFactory.getLogger(DiskPersistence.class);

    private final Path path;
    private final OutputStream outputStream;
    private long currentTerm;
    private String voteFor;
    private List<LogEntry> entries = Lists.newArrayList();

    public DiskPersistence(Path persistencePath) throws IOException {
        this.path = checkNotNull(persistencePath, "persistence path");
        this.currentTerm = 0L;
        this.voteFor = null;

        if (!Files.exists(path)) {
            Files.createFile(path);
            outputStream = new FileOutputStream(path.toFile());
        } else {
            recover();
            outputStream = new FileOutputStream(path.toFile(), true);
        }
    }

    private void recover() throws IOException {
        synchronized (this) {
            try (InputStream inputStream = new FileInputStream(path.toFile())) {
                int count = 0;
                while (inputStream.available() > 0) {
                    DiskPersistenceEntry entry = DiskPersistenceEntry.parseFrom(inputStream);
                    count++;
                    switch (entry.getPayloadCase()) {
                        case LOGENTRY:
                            entries.add(entry.getLogEntry());
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
    }

    @Override
    public synchronized long getCurrentTerm() {
        return currentTerm;
    }

    @Override
    public synchronized long incrementAndGetCurrentTerm() {
        final long newTerm = currentTerm + 1;
        DiskPersistenceEntry diskPersistenceEntry = DiskPersistenceEntry.newBuilder()
                .setCurrentTerm(newTerm)
                .build();
        try {
            diskPersistenceEntry.writeTo(outputStream);
            currentTerm = newTerm;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return currentTerm;
    }

    @Nullable
    @Override
    public synchronized String getVotedFor() {
        return voteFor;
    }

    @Override
    public synchronized boolean compareAndSetVoteFor(@Nullable String old, @Nullable String vote) {
        if (Objects.equals(old, voteFor)) {
            DiskPersistenceEntry diskPersistenceEntry = DiskPersistenceEntry.newBuilder()
                    .setVotedFor(vote)
                    .build();
            try {
                diskPersistenceEntry.writeTo(outputStream);
                voteFor = vote;
                return true;
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
        return false;
    }

    @Override
    public synchronized LogEntry getLogEntry(int index) {
        return entries.get(index);
    }

    @Override
    public synchronized int getLogEntrySize() {
        return entries.size();
    }

    @Override
    public synchronized void appendLogEntry(LogEntry logEntry) {
        DiskPersistenceEntry diskPersistenceEntry = DiskPersistenceEntry.newBuilder()
                .setLogEntry(checkNotNull(logEntry, "log entry"))
                .build();
        try {
            diskPersistenceEntry.writeTo(outputStream);
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
