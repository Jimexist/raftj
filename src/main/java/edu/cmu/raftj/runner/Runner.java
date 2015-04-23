package edu.cmu.raftj.runner;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ServiceManager;
import edu.cmu.raftj.persistence.FilePersistence;
import edu.cmu.raftj.persistence.Persistence;
import edu.cmu.raftj.rpc.DefaultCommunicator;
import edu.cmu.raftj.server.DefaultServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
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
        return Long.parseLong(System.getProperty("raftj.election.timeout", "130"));
    }

    private static ImmutableSet<HostAndPort> getServersList() {
        ImmutableSet<HostAndPort> result = FluentIterable.of(System.getProperty("raftj.servers", "").split(","))
                .transform(HostAndPort::fromString).toSet();
        checkArgument(all(result, (hostport) -> hostport.hasPort() && !hostport.getHostText().isEmpty()),
                "not all server hostports are valid: %s", result);
        return result;
    }

    private static void loadPropertyFileFromJar(String filename) throws IOException {
        System.getProperties().load(Runner.class.getClassLoader().getResourceAsStream(filename));
    }

    private static void loadPropertyFile(Path path) throws IOException {
        Reader reader = Files.newReader(path.toFile(), Charset.defaultCharset());
        System.getProperties().load(reader);
    }

    public static void main(String[] args) throws IOException {
        checkArgument(args.length == 2 || args.length == 3, "usage: <hostport> <log_file> <prop_file?>");

        if (args.length == 3) {
            loadPropertyFile(Paths.get(args[2]));
        } else {
            loadPropertyFileFromJar("config.properties");
        }

        final Set<HostAndPort> servers = getServersList();
        final HostAndPort hostAndPort = HostAndPort.fromString(args[0]);

        checkArgument(!servers.isEmpty(), "server list cannot be empty");
        checkArgument(servers.size() % 2 == 1, "must be odd number"); // TODO - is this true?
        checkArgument(servers.contains(hostAndPort),
                "host and port %s not in the given servers list %s",
                hostAndPort, servers);

        logger.info("server lists {}, this server is {}", servers, hostAndPort);

        final DefaultCommunicator communicator = new DefaultCommunicator(hostAndPort, Sets.difference(servers, ImmutableSet.of(hostAndPort)));
        final Persistence persistence = new FilePersistence(Paths.get(args[1]));
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
