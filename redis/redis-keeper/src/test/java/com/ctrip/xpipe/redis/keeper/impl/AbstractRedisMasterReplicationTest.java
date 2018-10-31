package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointSelector;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import static com.ctrip.xpipe.redis.keeper.impl.AbstractRedisMasterReplication.KEY_MASTER_CONNECT_RETRY_DELAY_SECONDS;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Jul 11, 2018
 */

@RunWith(MockitoJUnitRunner.class)
public class AbstractRedisMasterReplicationTest extends AbstractRedisKeeperTest {

    @Mock
    private RedisMaster redisMaster;

    @Mock
    private RedisKeeperServer redisKeeperServer;

    @Mock
    private ReplicationStore replicationStore;

    @Mock
    private MetaStore metaStore;

    private NioEventLoopGroup nioEventLoopGroup;

    private AbstractRedisMasterReplication redisMasterReplication;

    private int replTimeoutMilli = 6000;

    @Before
    public void beforeDefaultRedisMasterReplicationTest() throws Exception {

        MockitoAnnotations.initMocks(this);

        nioEventLoopGroup = new NioEventLoopGroup();

        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));

        when(redisMaster.getCurrentReplicationStore()).thenReturn(replicationStore);
        when(replicationStore.getMetaStore()).thenReturn(metaStore);
    }

    @Test
    public void testListeningPortCommandTimeOut() throws Exception {

        System.setProperty(KEY_MASTER_CONNECT_RETRY_DELAY_SECONDS, "0");
        Server server = startEmptyServer();
        ProxyConnectProtocol protocol = new DefaultProxyConnectProtocolParser().read("PROXY ROUTE TCP://127.0.0.1:"+server.getPort());
        ProxyEnabledEndpoint endpoint = new ProxyEnabledEndpoint("127.0.0.1", server.getPort(), protocol);

        when(redisMaster.masterEndPoint()).thenReturn(endpoint);
        ProxyResourceManager proxyEndpointManager = mock(ProxyResourceManager.class);
        ProxyEndpointSelector selector = mock(ProxyEndpointSelector.class);
        when(proxyEndpointManager.createProxyEndpointSelector(any())).thenReturn(selector);
        when(selector.nextHop()).thenReturn(protocol.nextEndpoints().get(0));

        DefaultRedisMasterReplication.PROXYED_REPLICATION_COMMAND_TIMEOUT_MILLI = 5;
        redisMasterReplication = new DefaultRedisMasterReplication(redisMaster, redisKeeperServer, nioEventLoopGroup,
                scheduled, replTimeoutMilli, proxyEndpointManager);

        redisMasterReplication = spy(redisMasterReplication);
        redisMasterReplication.initialize();
        redisMasterReplication.start();


        verify(redisMasterReplication, timeout(500).atLeast(2)).connectWithMaster();
    }
}