package edu.cmu.raftj.runner;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import edu.cmu.raftj.server.DefaultServer;
import edu.cmu.raftj.server.Server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by jiayu on 3/19/15.
 */
public final class Runner {

    private Runner() {
    }

    private static long getElectionTimeout() {
        return Long.parseLong(System.getProperty("raftj.election.timeout", "1000"));
    }

    private static ImmutableList<HostAndPort> getServersList() {
        return FluentIterable.of(System.getProperty("raftj.servers", "").split(","))
                .transform(HostAndPort::fromString).toList();
    }

    private static HostAndPort getServerHostAndPort() {
        return HostAndPort.fromString(System.getProperty("raftj.hostport", ""));
    }

    public static void main(String[] args) {
        checkArgument(args.length == 1, "usage: <config_file_path>");
        String configFilePath = args[0];
        Path path = Paths.get(configFilePath);
        checkArgument(Files.exists(path) && Files.isRegularFile(path) && Files.isReadable(path),
                "%s is not a regular readable file", path);

        Server server = new DefaultServer(getServerHostAndPort(), getServersList(), getElectionTimeout());

    }
}
