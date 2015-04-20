package edu.cmu.raftj.server;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ServiceManager;
import edu.cmu.raftj.rpc.Communicator;
import edu.cmu.raftj.rpc.Messages.AppendEntriesRequest;
import edu.cmu.raftj.rpc.Messages.VoteRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;


/**
 * Created by jiayu on 4/20/15.
 */
public class DefaultServerTest {

    @Mock
    private Communicator communicator;
    private DefaultServer defaultServer;
    private ServiceManager serviceManager;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        defaultServer = new DefaultServer(42L, communicator);
        serviceManager = new ServiceManager(ImmutableList.of(defaultServer));
        serviceManager.startAsync().awaitHealthy();

        assertEquals(Server.Role.Follower, defaultServer.getCurrentRole());
        assertEquals(0, defaultServer.getCurrentTerm());
        assertEquals(42L, defaultServer.getElectionTimeout());
    }

    @After
    public void tearDown() throws Exception {
        serviceManager.stopAsync().awaitStopped(1000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testOnVoteRequest() throws Exception {
        VoteRequest request = VoteRequest.newBuilder()
                .setCandidateId("candid-id")
                .setCandidateTerm(1L)
                .setLastLogIndex(20L)
                .setLastLogTerm(1L)
                .build();

        defaultServer.onVoteRequest(request);
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