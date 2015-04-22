package edu.cmu.raftj.runner;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ServiceManager;
import edu.cmu.raftj.persistence.DiskPersistence;
import edu.cmu.raftj.persistence.Persistence;
import edu.cmu.raftj.rpc.DefaultCommunicator;
import edu.cmu.raftj.server.DefaultServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.all;

/**
 * Main entrance to the program.
 */
public final class Runner {

    private static final Logger logger = LoggerFactory.getLogger(Runner.class);

    private Runner() {
    }

    private static long getElectionTimeout() {
        return Long.parseLong(System.getProperty("raftj.election.timeout", "1000"));
    }

    private static ImmutableSet<HostAndPort> getServersList() {
        ImmutableSet<HostAndPort> result = FluentIterable.of(System.getProperty("raftj.servers", "").split(","))
                .transform(HostAndPort::fromString).toSet();
        checkArgument(all(result, (hostport) -> hostport.hasPort() && !hostport.getHostText().isEmpty()),
                "not all server hostports are valid: %s", result);
        return result;
    }

    private static HostAndPort getServerHostAndPort() {
        return HostAndPort.fromString(System.getProperty("raftj.hostport"));
    }

    public static void main(String[] args) throws IOException {
        checkArgument(args.length == 1, "usage: <config_file_path>");
        String configFilePath = args[0];
        Path path = Paths.get(configFilePath);

        final Set<HostAndPort> servers = getServersList();
        final HostAndPort hostAndPort = getServerHostAndPort();

        checkArgument(!servers.isEmpty(), "server list cannot be empty");
        checkArgument(servers.size() % 2 == 1, "must be odd number"); // TODO - is this true?
        checkArgument(servers.contains(hostAndPort),
                "host and port %s not in the given servers list %s",
                hostAndPort, servers);

        logger.info("server lists {}, this server is {}", servers, hostAndPort);

        final DefaultCommunicator communicator = new DefaultCommunicator(hostAndPort, Sets.difference(servers, ImmutableSet.of(hostAndPort)));
        final Persistence persistence = new DiskPersistence(path);
        final DefaultServer server = new DefaultServer(getElectionTimeout(), communicator, persistence);
        communicator.setRequestListener(server);

        final ServiceManager serviceManager = new ServiceManager(ImmutableList.of(server, communicator));
        serviceManager.startAsync();
        serviceManager.awaitHealthy();

        // shutdown - TODO, wait
        serviceManager.stopAsync();
        try {
            serviceManager.awaitStopped(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            logger.error("failed to shutdown", e);
        }

    }
}
