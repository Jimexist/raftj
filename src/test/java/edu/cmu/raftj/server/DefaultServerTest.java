package edu.cmu.raftj.server;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.google.common.util.concurrent.SettableFuture;
import edu.cmu.raftj.persistence.FilePersistence;
import edu.cmu.raftj.persistence.Persistence;
import edu.cmu.raftj.rpc.Communicator;
import edu.cmu.raftj.rpc.Messages.AppendEntriesRequest;
import edu.cmu.raftj.rpc.Messages.AppendEntriesResponse;
import edu.cmu.raftj.rpc.Messages.VoteRequest;
import edu.cmu.raftj.rpc.Messages.VoteResponse;
import edu.cmu.raftj.server.Server.Role;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;

import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static edu.cmu.raftj.server.Server.Role.Follower;
import static edu.cmu.raftj.server.Server.Role.Leader;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;


/**
 * Test for {@link DefaultServer}.
 */
@RunWith(JUnit4.class)
public class DefaultServerTest {

    @Mock
    private Communicator communicator;

    private DefaultServer defaultServer;
    private ServiceManager serviceManager;

    private void mockImmediateVoteResponse() {
        VoteResponse voteResponse = VoteResponse.newBuilder()
                .setTerm(1L)
                .setVoteGranted(true)
                .build();
        when(communicator.sendVoteRequest(any(VoteRequest.class))).thenReturn(
                immediateFuture(ImmutableList.of(voteResponse, voteResponse, voteResponse, voteResponse)));
    }

    private SettableFuture<List<VoteResponse>> mockFutureVoteResponse() {
        SettableFuture<List<VoteResponse>> result = SettableFuture.create();
        when(communicator.sendVoteRequest(any(VoteRequest.class))).thenAnswer(invocation -> result);
        return result;
    }

    private void mockImmediateAppendResponse() {
        AppendEntriesResponse appendEntriesResponse = AppendEntriesResponse.newBuilder()
                .setSuccess(true)
                .setTerm(1L)
                .build();
        when(communicator.sendAppendEntriesRequest(any(AppendEntriesRequest.class))).thenReturn(
                immediateFuture(ImmutableList.of(appendEntriesResponse)));
    }

    @Before
    public void setUp() throws Exception {
        initMocks(this);

        when(communicator.getServerHostAndPort()).thenReturn(HostAndPort.fromParts("localhost", 7654));

        final Persistence persistence = new FilePersistence(Files.createTempFile("prefix_", ".log"));
        defaultServer = new DefaultServer(42L, communicator, persistence);
        serviceManager = new ServiceManager(ImmutableList.of(defaultServer));
        serviceManager.addListener(new ServiceManager.Listener() {
            @Override
            public void failure(Service service) {
                System.exit(-1);
            }
        }, directExecutor());

        assertEquals(Follower, defaultServer.getCurrentRole());
        assertEquals(0, defaultServer.getCurrentTerm());
    }

    @After
    public void tearDown() throws Exception {
        serviceManager.stopAsync().awaitStopped(1L, TimeUnit.SECONDS);
        reset(communicator);
    }

    @Test
    public void testOnVoteRequest() throws Exception {

        mockImmediateVoteResponse();
        mockImmediateAppendResponse();
        serviceManager.startAsync().awaitHealthy();

        VoteRequest.Builder builder = VoteRequest.newBuilder()
                .setCandidateId("candid-id")
                .setCandidateTerm(1000L) // a very large value
                .setLastLogIndex(1L)
                .setLastLogTerm(1L);
        assertTrue(defaultServer.onVoteRequest(builder.build()).getVoteGranted());
        assertTrue(defaultServer.onVoteRequest(builder.build()).getVoteGranted());
        assertFalse(defaultServer.onVoteRequest(builder.setCandidateId("lol").build()).getVoteGranted());
        assertEquals(Follower, defaultServer.getCurrentRole());
    }

    @Test
    public void testOnFollowerConvert() throws Exception {

        mockImmediateVoteResponse();
        mockImmediateAppendResponse();
        serviceManager.startAsync().awaitHealthy();

        VoteRequest.Builder builder = VoteRequest.newBuilder()
                .setCandidateId("candid-id")
                .setCandidateTerm(2L) // a rather small value
                .setLastLogIndex(1L)
                .setLastLogTerm(1L);
        while (defaultServer.getCurrentRole() != Leader) {

        }
        assertTrue(defaultServer.onVoteRequest(builder.build()).getVoteGranted());
        while (defaultServer.getCurrentRole() != Follower) {

        }
        assertEquals(2, defaultServer.getCurrentTerm());
        while (defaultServer.getCurrentRole() != Leader) {

        }
        assertEquals(3, defaultServer.getCurrentTerm());
    }

    @Test
    public void testVoteRetry() throws Exception {
        mockImmediateAppendResponse();
        SettableFuture<List<VoteResponse>> result  = mockFutureVoteResponse();
        serviceManager.startAsync().awaitHealthy();

        verify(communicator, timeout(5000L).atLeast(3)).sendVoteRequest(any(VoteRequest.class));
        assertEquals(Role.Candidate, defaultServer.getCurrentRole());
        verify(communicator, never()).sendAppendEntriesRequest(any(AppendEntriesRequest.class));

        // vote yes
        result.set(ImmutableList.of(VoteResponse.newBuilder().setTerm(1L).setVoteGranted(true).build()));
        while (defaultServer.getCurrentRole() == Role.Candidate) {
            // loop
        }
        assertEquals(Leader, defaultServer.getCurrentRole());
        verify(communicator, timeout(1000L).atLeast(1)).sendAppendEntriesRequest(any(AppendEntriesRequest.class));
    }

    @Test
    public void testVoteRetryWithHigherTerm() throws Exception {
        mockImmediateAppendResponse();
        SettableFuture<List<VoteResponse>> result  = mockFutureVoteResponse();
        serviceManager.startAsync().awaitHealthy();

        VoteRequest voteRequest = VoteRequest.newBuilder()
                .setCandidateId("lol")
                .setCandidateTerm(1000L)
                .setLastLogIndex(1L)
                .setLastLogTerm(1L)
                .build();

        defaultServer.onVoteRequest(voteRequest);

        verify(communicator, timeout(1000L).atLeast(3)).sendVoteRequest(any(VoteRequest.class));

        assertEquals(Role.Candidate, defaultServer.getCurrentRole());
        result.set(ImmutableList.of(VoteResponse.newBuilder().setTerm(10000L).setVoteGranted(true).build()));
        verify(communicator, never()).sendAppendEntriesRequest(any(AppendEntriesRequest.class));
        while (defaultServer.getCurrentRole() == Role.Candidate) {
            // loop
        }
        assertEquals(Follower, defaultServer.getCurrentRole());
    }

    @Test
    public void testOnAppendEntriesRequest() throws Exception {

        mockImmediateVoteResponse();
        mockImmediateAppendResponse();
        serviceManager.startAsync().awaitHealthy();

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