package edu.cmu.raftj.rpc;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.*;
import com.google.protobuf.GeneratedMessage;
import edu.cmu.raftj.rpc.Messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.*;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newWorkStealingPool;

/**
 * Default {@link Communicator} implementation that runs server on a dedicated thread, and boardCasts
 * requests concurrently. This implementation does not reuse socket and connections.
 */
public class DefaultCommunicator extends AbstractExecutionThreadService implements Communicator {

    private static final Logger logger = LoggerFactory.getLogger(DefaultCommunicator.class);

    private final HostAndPort hostAndPort;
    private final ImmutableSet<HostAndPort> audience;
    private final ListeningExecutorService boardCastExecutor = listeningDecorator(newWorkStealingPool());
    private final ServerSocket serverSocket = new ServerSocket();
    private RequestListener requestListener;

    public DefaultCommunicator(HostAndPort hostAndPort, Set<HostAndPort> audience) throws IOException {
        this.hostAndPort = checkNotNull(hostAndPort, "host and port");
        this.audience = ImmutableSet.copyOf(audience);
        checkArgument(!audience.contains(hostAndPort),
                "server audiences %s cannot contain this server %s",
                audience, hostAndPort);
    }

    private <T extends GeneratedMessage> ListenableFuture<? extends Collection<T>> boardCast(Request request, Function<InputStream, T> builder) {
        final List<ListenableFuture<T>> list = Lists.newArrayList();
        for (HostAndPort hostAndPort : audience) {
            final SettableFuture<T> settableFuture = SettableFuture.create();
            boardCastExecutor.execute(() -> {
                try (final Socket socket = new Socket(InetAddress.getByName(hostAndPort.getHostText()), hostAndPort.getPort());
                     final OutputStream outputStream = socket.getOutputStream();
                     final InputStream inputStream = socket.getInputStream()) {
                    request.writeTo(outputStream);
                    settableFuture.set(builder.apply(inputStream));
                } catch (Exception e) {
                    logger.warn("error in sending vote request to {}, exception {}",
                            hostAndPort, Throwables.getStackTraceAsString(e));
                    settableFuture.setException(e);
                }
            });
            list.add(settableFuture);
        }
        return Futures.allAsList(list);
    }

    @Override
    public ListenableFuture<? extends Collection<VoteResponse>> sendVoteRequest(VoteRequest voteRequest) {
        return boardCast(Request.newBuilder().setVoteRequest(voteRequest).build(), (is) -> {
            try {
                return VoteResponse.parseFrom(is);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        });
    }

    @Override
    public ListenableFuture<? extends Collection<AppendEntriesResponse>> sendAppendEntriesRequest(AppendEntriesRequest appendEntriesRequest) {
        return boardCast(Request.newBuilder().setAppendEntriesRequest(appendEntriesRequest).build(), (is) -> {
            try {
                return AppendEntriesResponse.parseFrom(is);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        });
    }

    @Override
    public void setRequestListener(RequestListener requestListener) {
        checkState(!isRunning(), "must be set before run");
        this.requestListener = requestListener;
    }

    @Override
    protected void startUp() throws Exception {
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPort()));
    }

    @Override
    protected void run() throws Exception {
        checkState(null != requestListener, "request listener not set");
        while (isRunning()) {
            try (final Socket client = serverSocket.accept();
                 final InputStream inputStream = client.getInputStream()) {
                final Request request = Request.parseFrom(inputStream);
                logger.info("handling {} request from {}:{}",
                        request.getPayloadCase(), client.getInetAddress(), client.getPort());
                switch (request.getPayloadCase()) {
                    case APPENDENTRIESREQUEST:
                        requestListener.onAppendEntriesRequest(request.getAppendEntriesRequest());
                        break;
                    case VOTEREQUEST:
                        requestListener.onVoteRequest(request.getVoteRequest());
                        break;
                    default:
                        throw new IllegalArgumentException("payload not set");
                }
            } catch (Exception e) {
                logger.warn("exception in accepting client", e);
            }
        }
    }

    @Override
    protected void shutDown() throws Exception {
        serverSocket.close();
    }
}
