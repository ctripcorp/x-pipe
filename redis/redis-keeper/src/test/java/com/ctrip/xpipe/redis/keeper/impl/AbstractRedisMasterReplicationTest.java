package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.core.protocal.error.RedisError;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointSelector;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import static com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand.KEY_PROXYED_REDIS_COMMAND_TIME_OUT_MILLI;
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
        KeeperResourceManager proxyEndpointManager = getRegistry().getComponent(KeeperResourceManager.class);

        String origin_timeout = System.getProperty(KEY_PROXYED_REDIS_COMMAND_TIME_OUT_MILLI);
        System.setProperty(KEY_PROXYED_REDIS_COMMAND_TIME_OUT_MILLI, "5");
        redisMasterReplication = new DefaultRedisMasterReplication(redisMaster, redisKeeperServer, nioEventLoopGroup,
                scheduled, replTimeoutMilli, proxyEndpointManager);

        redisMasterReplication = spy(redisMasterReplication);
        redisMasterReplication.initialize();
        redisMasterReplication.start();


        verify(redisMasterReplication, timeout(500).atLeast(2)).connectWithMaster();

        System.setProperty(KEY_PROXYED_REDIS_COMMAND_TIME_OUT_MILLI, origin_timeout);
    }

    @Test
    public void testCheckKeeper() throws Exception {
        Server server = startServer("-ERR syntax error");
        SimpleObjectPool<NettyClient> pool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultProxyEndpoint("127.0.0.1", server.getPort()));
        Replconf replconf = new Replconf(pool, Replconf.ReplConfType.KEEPER, scheduled, 100);
        replconf.execute().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if (!commandFuture.isSuccess()) {
                    logger.error("[]", commandFuture.cause());
                    Assert.assertTrue(commandFuture.cause() instanceof RedisError);
                }
            }
        });
        replconf.future().await();
        server.stop();
    }

    @Test
    public void testCheckKeeperWithTimeout() throws Exception {
        SimpleObjectPool<NettyClient> pool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultProxyEndpoint(getTimeoutIp(), 0));
        Replconf replconf = new Replconf(pool, Replconf.ReplConfType.KEEPER, scheduled, 100);
        replconf.execute().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if (!commandFuture.isSuccess()) {
                    logger.error("[]", commandFuture.cause());
                    Assert.assertTrue(commandFuture.cause() instanceof CommandTimeoutException);
                }
            }
        });
        replconf.future().await();
    }

    @Test
    public void testCheckKeeperWithConnectRefused() throws Exception {
        SimpleObjectPool<NettyClient> pool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultProxyEndpoint("127.0.0.1", randomPort()));
        Replconf replconf = new Replconf(pool, Replconf.ReplConfType.KEEPER, scheduled, 100);
        replconf.execute().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if (!commandFuture.isSuccess()) {
                    logger.error("[]", commandFuture.cause());
                    Assert.assertTrue(commandFuture.cause() instanceof CommandTimeoutException);
                }
            }
        });
        replconf.future().await();
    }
}