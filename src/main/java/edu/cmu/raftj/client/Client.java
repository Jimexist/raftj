package edu.cmu.raftj.client;

import com.google.common.net.HostAndPort;
import edu.cmu.raftj.rpc.Messages;
import edu.cmu.raftj.rpc.Messages.ClientMessageResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * client
 */
public final class Client {

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private static ClientMessageResponse sendCommand(HostAndPort hostAndPort, String command) throws IOException {
        try (Socket socket = new Socket(InetAddress.getByName(hostAndPort.getHostText()), hostAndPort.getPort());
             InputStream inputStream = socket.getInputStream();
             OutputStream outputStream = socket.getOutputStream()) {
            Messages.Request.newBuilder().setCommand(command).build().writeDelimitedTo(outputStream);
            return ClientMessageResponse.parseDelimitedFrom(inputStream);
        }
    }

    public static void main(String[] args) throws IOException {
        checkArgument(args.length > 0, "usage: <server_host_port>");
        HostAndPort hostAndPort = HostAndPort.fromString(args[0]);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            while (true) {
                String line = reader.readLine();
                if (null == line) {
                    break;
                }
                line = line.trim();
                logger.info("sending command {} to {}", line, hostAndPort);
                while (true) {
                    ClientMessageResponse response = sendCommand(hostAndPort, line);
                    if (response == null) {
                        logger.warn("null message received, retry");
                    } else if (response.getSuccess()) {
                        break;
                    } else if (!"".equals(response.getLeaderID())) {
                        hostAndPort = HostAndPort.fromString(response.getLeaderID());
                        logger.warn("resending {} to {} instead", line, hostAndPort);
                    }
                }
            }
        }
    }
}

