package edu.cmu.raftj.rpc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ServiceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * Created by jiayu on 4/20/15.
 */
public class DefaultCommunicatorTest {

    @Mock
    private RequestListener requestListener;
    @Mock
    private ServerSocket serverSocket;
    @Mock
    private Socket clientSocket;
    @Mock
    private InputStream clientInputStream;
    @Mock
    private OutputStream clientOutputStream;
    private DefaultCommunicator defaultCommunicator;
    private ServiceManager serviceManager;

    @Before
    public void setUp() throws Exception {
        initMocks(this);

        when(serverSocket.getInetAddress()).thenReturn(InetAddress.getLocalHost());
        when(serverSocket.getLocalPort()).thenReturn(13345);
        when(serverSocket.isBound()).thenReturn(true);
        when(serverSocket.accept()).thenReturn(clientSocket);

        when(clientSocket.getInputStream()).thenReturn(clientInputStream);
        when(clientSocket.getOutputStream()).thenReturn(clientOutputStream);

        defaultCommunicator = new DefaultCommunicator(
                serverSocket,
                ImmutableSet.of(HostAndPort.fromParts("localhost", 13346)));
        defaultCommunicator.setRequestListener(requestListener);
        serviceManager = new ServiceManager(ImmutableList.of(defaultCommunicator));
        serviceManager.startAsync().awaitHealthy();
    }

    @After
    public void tearDown() throws Exception {
        serviceManager.stopAsync().awaitStopped(1L, TimeUnit.SECONDS);
    }

    @Test
    public void testSendVoteRequest() throws Exception {

    }

    @Test
    public void testSendAppendEntriesRequest() throws Exception {

    }

}