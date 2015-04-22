package edu.cmu.raftj.server;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import edu.cmu.raftj.persistence.Persistence;
import edu.cmu.raftj.rpc.Communicator;
import edu.cmu.raftj.rpc.Messages.AppendEntriesRequest;
import edu.cmu.raftj.rpc.Messages.VoteRequest;
import edu.cmu.raftj.rpc.Messages.VoteResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;


/**
 * Test for {@link DefaultServer}.
 */
public class DefaultServerTest {

    @Mock
    private Communicator communicator;
    @Mock
    private Persistence persistence;
    private DefaultServer defaultServer;
    private ServiceManager serviceManager;

    @Before
    public void setUp() throws Exception {
        initMocks(this);

        when(communicator.getServerHostAndPort()).thenReturn(HostAndPort.fromParts("localhost", 7654));

        VoteResponse response = VoteResponse.newBuilder().setTerm(1L).setVoteGranted(true).build();
        when(communicator.sendVoteRequest(any(VoteRequest.class))).thenReturn(
                immediateFuture(ImmutableList.of(response)));

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