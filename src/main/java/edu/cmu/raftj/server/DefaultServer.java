package edu.cmu.raftj.server;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by jiayu on 3/19/15.
 */
public class DefaultServer implements Server {

    private final AtomicLong currentTerm = new AtomicLong(0);
    private final AtomicReference<Role> currentRole = new AtomicReference<>(Role.Follower);
    private final HostAndPort hostAndPort;
    private final ImmutableList<HostAndPort> serversList;

    public DefaultServer(HostAndPort hostAndPort, List<HostAndPort> serversList) {
        this.hostAndPort = hostAndPort;
        this.serversList = ImmutableList.copyOf(serversList);
    }

}
