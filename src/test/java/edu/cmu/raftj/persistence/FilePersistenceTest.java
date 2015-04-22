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
public class FilePersistenceTest {

    private Path path;

    @Before
    public void setUp() throws Exception {
        path = Files.createTempFile("temp_file_", ".log");
    }

    @Test
    public void testCurrentTerms() throws Exception {
        try (FilePersistence filePersistence = new FilePersistence(path)) {
            assertEquals(0L, filePersistence.getCurrentTerm());
            assertEquals(1L, filePersistence.incrementAndGetCurrentTerm());
        }

        try (FilePersistence another = new FilePersistence(path)) {
            assertEquals(1L, another.getCurrentTerm());
        }
    }


    @Test
    public void testGetVotedFor() throws Exception {
        try (FilePersistence filePersistence = new FilePersistence(path)) {
            assertEquals(null, filePersistence.getVotedFor());
            assertEquals(true, filePersistence.compareAndSetVoteFor(null, "lol"));
            assertEquals(false, filePersistence.compareAndSetVoteFor(null, "lol"));
            assertEquals(true, filePersistence.compareAndSetVoteFor("lol", "hello"));
        }

        try (FilePersistence another = new FilePersistence(path)) {
            assertEquals("hello", another.getVotedFor());
        }
    }

    @Test
    public void testGetLogEntry() throws Exception {
        try (FilePersistence filePersistence = new FilePersistence(path)) {
            assertEquals(0, filePersistence.getLogEntrySize());
            Messages.LogEntry logEntry = Messages.LogEntry.newBuilder().setTerm(10L).setCommand("x = 1").build();
            filePersistence.appendLogEntry(logEntry);
            assertEquals(1, filePersistence.getLogEntrySize());
        }

        try (FilePersistence filePersistence = new FilePersistence(path)) {
            assertEquals(1, filePersistence.getLogEntrySize());
            Messages.LogEntry logEntry = Messages.LogEntry.newBuilder().setTerm(10L).setCommand("x = 1").build();
            assertEquals(logEntry, filePersistence.getLogEntry(0));
        }

    }

}