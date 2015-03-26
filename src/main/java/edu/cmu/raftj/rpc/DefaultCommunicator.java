package edu.cmu.raftj.rpc;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractIdleService;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by jiayu on 3/26/15.
 */
public class DefaultCommunicator extends AbstractIdleService implements Communicator {

    private final HostAndPort hostAndPort;
    private final RequestListener listener;
    private final ImmutableSet<HostAndPort> audience;
    private final ExecutorService executorService;

    public DefaultCommunicator(HostAndPort hostAndPort, RequestListener listener, Set<HostAndPort> audience) {
        this.listener = checkNotNull(listener);
        this.hostAndPort = hostAndPort;
        this.audience = ImmutableSet.copyOf(audience);
        this.executorService = Executors.newWorkStealingPool();
    }

    @Override
    public void sendVoteRequest(Messages.VoteRequest voteRequest) {
        for (HostAndPort hostAndPort : audience) {

        }
    }

    @Override
    public void sendAppendEntriesRequest(Messages.AppendEntriesRequest appendEntriesRequest) {

    }

    @Override
    protected void startUp() throws Exception {

    }

    @Override
    protected void shutDown() throws Exception {

    }
}
