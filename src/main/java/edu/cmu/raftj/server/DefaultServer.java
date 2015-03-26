package edu.cmu.raftj.server;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import edu.cmu.raftj.rpc.Communicator;
import edu.cmu.raftj.rpc.DefaultCommunicator;
import edu.cmu.raftj.rpc.RequestListener;
import edu.cmu.raftj.rpc.Messages;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by jiayu on 3/19/15.
 */
public class DefaultServer implements Server {

    private final AtomicLong currentTerm = new AtomicLong(0);
    private final AtomicReference<Role> currentRole = new AtomicReference<>(Role.Follower);
    private final HostAndPort hostAndPort;
    private final ImmutableSet<HostAndPort> servers;
    private final long electionTimeout;
    private final Communicator communicator;
    private final RequestListener requestListener = new RequestListener() {
        @Override
        public void onVoteRequest(Messages.VoteRequest voteRequest) {

        }

        @Override
        public void onAppendEntriesRequest(Messages.AppendEntriesRequest appendEntriesRequest) {

        }
    };

    public DefaultServer(HostAndPort hostAndPort, Set<HostAndPort> servers, long electionTimeout) {
        checkArgument(electionTimeout > 0, "election timeout must be positive");
        checkArgument(!servers.isEmpty(), "server list cannot be empty");
        checkArgument(servers.size() % 2 == 1, "must be odd number");
        checkArgument(servers.contains(hostAndPort),
                "host and port %s not in the given servers list %s",
                hostAndPort, servers);
        this.hostAndPort = checkNotNull(hostAndPort, "host and port");
        this.servers = ImmutableSet.copyOf(servers);
        this.electionTimeout = electionTimeout;
        this.communicator = new DefaultCommunicator(hostAndPort, requestListener,
                Sets.difference(servers, ImmutableSet.of(hostAndPort)));
    }
}
