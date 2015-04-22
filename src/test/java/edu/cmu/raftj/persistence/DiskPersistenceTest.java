package edu.cmu.raftj.persistence;

import edu.cmu.raftj.rpc.Messages;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

/**
 * test for persistence
 */
public class DiskPersistenceTest {

    private Path path;

    @Before
    public void setUp() throws Exception {
        path = Files.createTempFile("temp_file_", ".log");
    }

    @Test
    public void testCurrentTerms() throws Exception {
        try (DiskPersistence diskPersistence = new DiskPersistence(path)) {
            assertEquals(0L, diskPersistence.getCurrentTerm());
            assertEquals(1L, diskPersistence.incrementAndGetCurrentTerm());
        }

        try (DiskPersistence another = new DiskPersistence(path)) {
            assertEquals(1L, another.getCurrentTerm());
        }
    }


    @Test
    public void testGetVotedFor() throws Exception {
        try (DiskPersistence diskPersistence = new DiskPersistence(path)) {
            assertEquals(null, diskPersistence.getVotedFor());
            assertEquals(true, diskPersistence.compareAndSetVoteFor(null, "lol"));
            assertEquals(false, diskPersistence.compareAndSetVoteFor(null, "lol"));
            assertEquals(true, diskPersistence.compareAndSetVoteFor("lol", "hello"));
        }

        try (DiskPersistence another = new DiskPersistence(path)) {
            assertEquals("hello", another.getVotedFor());
        }
    }

    @Test
    public void testGetLogEntry() throws Exception {
        try (DiskPersistence diskPersistence = new DiskPersistence(path)) {
            assertEquals(0, diskPersistence.getLogEntrySize());
            Messages.LogEntry logEntry = Messages.LogEntry.newBuilder().setTerm(10L).setCommand("x = 1").build();
            diskPersistence.appendLogEntry(logEntry);
            assertEquals(1, diskPersistence.getLogEntrySize());
        }

        try (DiskPersistence diskPersistence = new DiskPersistence(path)) {
            assertEquals(1, diskPersistence.getLogEntrySize());
            Messages.LogEntry logEntry = Messages.LogEntry.newBuilder().setTerm(10L).setCommand("x = 1").build();
            assertEquals(logEntry, diskPersistence.getLogEntry(0));
        }

    }

}