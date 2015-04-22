package edu.cmu.raftj.server;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.google.common.util.concurrent.SettableFuture;
import edu.cmu.raftj.persistence.FilePersistence;
import edu.cmu.raftj.persistence.Persistence;
import edu.cmu.raftj.rpc.Communicator;
import edu.cmu.raftj.rpc.Messages;
import edu.cmu.raftj.rpc.Messages.AppendEntriesRequest;
import edu.cmu.raftj.rpc.Messages.VoteRequest;
import edu.cmu.raftj.rpc.Messages.VoteResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;


/**
 * Test for {@link DefaultServer}.
 */
public class DefaultServerTest {

    @Mock
    private Communicator communicator;

    private DefaultServer defaultServer;
    private ServiceManager serviceManager;

    @Before
    public void setUp() throws Exception {
        initMocks(this);

        when(communicator.getServerHostAndPort()).thenReturn(HostAndPort.fromParts("localhost", 7654));

        VoteResponse voteRequest = VoteResponse.newBuilder()
                .setTerm(1L)
                .setVoteGranted(true)
                .build();
        when(communicator.sendVoteRequest(any(VoteRequest.class))).thenReturn(
                immediateFuture(ImmutableList.of(voteRequest)));

        Messages.AppendEntriesResponse appendEntriesResponse = Messages.AppendEntriesResponse.newBuilder()
                .setSuccess(true)
                .setTerm(1L)
                .build();
        when(communicator.sendAppendEntriesRequest(any(AppendEntriesRequest.class))).thenReturn(
                immediateFuture(ImmutableList.of(appendEntriesResponse)));

        Persistence persistence = new FilePersistence(Files.createTempFile("prefix_", ".log"));
        defaultServer = new DefaultServer(42L, communicator, persistence);
        serviceManager = new ServiceManager(ImmutableList.of(defaultServer));
        serviceManager.addListener(new ServiceManager.Listener() {
            @Override
            public void failure(Service service) {
                System.exit(-1);
            }
        }, MoreExecutors.directExecutor());
        serviceManager.startAsync().awaitHealthy();

        assertEquals(Server.Role.Follower, defaultServer.getCurrentRole());
        assertEquals(0, defaultServer.getCurrentTerm());
    }

    @After
    public void tearDown() throws Exception {
        serviceManager.stopAsync().awaitStopped(1L, TimeUnit.SECONDS);
    }

    @Test
    public void testOnVoteRequest() throws Exception {
        VoteRequest.Builder builder = VoteRequest.newBuilder()
                .setCandidateId("candid-id")
                .setCandidateTerm(1L)
                .setLastLogIndex(100L)
                .setLastLogTerm(1L);

        assertTrue(defaultServer.onVoteRequest(builder.build()).getVoteGranted());
        assertTrue(defaultServer.onVoteRequest(builder.build()).getVoteGranted());
        assertFalse(defaultServer.onVoteRequest(builder.setCandidateId("lol").build()).getVoteGranted());

    }

    @Test
    public void testVoteRetry() throws Exception {
        SettableFuture<List<VoteResponse>> result = SettableFuture.create();
        when(communicator.sendVoteRequest(any(VoteRequest.class))).thenAnswer(invocation -> result);
        verify(communicator, timeout(1000L).atLeast(3)).sendVoteRequest(any(VoteRequest.class));
        assertEquals(Server.Role.Candidate, defaultServer.getCurrentRole());
        result.set(ImmutableList.of(VoteResponse.newBuilder().setTerm(1L).setVoteGranted(true).build()));
        verify(communicator, never()).sendAppendEntriesRequest(any(AppendEntriesRequest.class));
        while (defaultServer.getCurrentRole() == Server.Role.Candidate) {
            // loop
        }
        assertEquals(Server.Role.Leader, defaultServer.getCurrentRole());
        verify(communicator, timeout(1000L).atLeast(1)).sendAppendEntriesRequest(any(AppendEntriesRequest.class));
    }


    @Test
    public void testVoteRetryWithHigherTerm() throws Exception {
        SettableFuture<List<VoteResponse>> result = SettableFuture.create();
        when(communicator.sendVoteRequest(any(VoteRequest.class))).thenAnswer(invocation -> result);
        verify(communicator, timeout(1000L).atLeast(3)).sendVoteRequest(any(VoteRequest.class));
        assertEquals(Server.Role.Candidate, defaultServer.getCurrentRole());
        result.set(ImmutableList.of(VoteResponse.newBuilder().setTerm(10000L).setVoteGranted(true).build()));
        verify(communicator, never()).sendAppendEntriesRequest(any(AppendEntriesRequest.class));
        while (defaultServer.getCurrentRole() == Server.Role.Candidate) {
            // loop
        }
        assertEquals(Server.Role.Follower, defaultServer.getCurrentRole());
    }

    @Test
    public void testOnAppendEntriesRequest() throws Exception {
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setLeaderId("leader")
                .setLeaderTerm(1L)
                .setLeaderCommitIndex(1L)
                .setPrevLogIndex(1L)
                .setPrevLogTerm(1L)
                .build();
        defaultServer.onAppendEntriesRequest(request);
    }
}