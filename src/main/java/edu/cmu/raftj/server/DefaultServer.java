package edu.cmu.raftj.server;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.FluentIterable.from;

/**
 * Created by jiayu on 3/19/15.
 */
public class DefaultServer implements Server {

    private final AtomicLong currentTerm = new AtomicLong(0);
    private final AtomicReference<Role> currentRole = new AtomicReference<>(Role.Follower);
    private final HostAndPort hostAndPort;
    private final ImmutableList<HostAndPort> serversList;
    private final ImmutableList<HostAndPort> otherServers;
    private final long electionTimeout;

    public DefaultServer(HostAndPort hostAndPort, List<HostAndPort> serversList, long electionTimeout) {
        checkArgument(electionTimeout > 0, "election timeout must be positive");
        checkArgument(!serversList.isEmpty(), "server list cannot be empty");
        checkArgument(serversList.size() % 2 == 1, "must be odd number");
        checkArgument(serversList.contains(hostAndPort),
                "host and port %s not in the given servers list %s",
                hostAndPort, serversList);
        this.hostAndPort = checkNotNull(hostAndPort, "host and port");
        this.serversList = ImmutableList.copyOf(serversList);
        this.electionTimeout = electionTimeout;
        this.otherServers = from(serversList).filter((s) -> !s.equals(hostAndPort)).toList();
    }


}
