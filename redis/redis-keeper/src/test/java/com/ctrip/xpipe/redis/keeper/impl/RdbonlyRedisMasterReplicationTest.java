package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.proxy.protocols.DefaultProxyConnectProtocol;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Jul 05, 2018
 */
@RunWith(MockitoJUnitRunner.class)
public class RdbonlyRedisMasterReplicationTest extends AbstractRedisKeeperTest {

    private RedisMaster keeperRedisMaster;

    private NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);

    private ReplicationStoreManager replicationStoreManager = createReplicationStoreManager();

    private DefaultEndPoint target;

    @Mock
    private DefaultRedisKeeperServer keeperServer;

    @Before
    public void beforeRdbonlyRedisMasterReplicationTest() throws IOException {
        MockitoAnnotations.initMocks(this);
        when(keeperServer.getRedisMaster()).thenReturn(keeperRedisMaster);
    }

    @Test
    public void testTimeoutMilli() throws CreateRdbDumperException {
        target = new DefaultEndPoint("localhost", randomPort());
        this.keeperRedisMaster = new DefaultRedisMaster(keeperServer, target, eventLoopGroup, replicationStoreManager, scheduled, mock(ProxyResourceManager.class));
        keeperRedisMaster.setMasterState(MASTER_STATE.REDIS_REPL_CONNECTED);
        RedisMasterNewRdbDumper dumper = (RedisMasterNewRdbDumper)keeperRedisMaster.createRdbDumper();
        RdbonlyRedisMasterReplication rdbonlyRedisMasterReplication = new RdbonlyRedisMasterReplication(keeperServer,
                keeperRedisMaster, eventLoopGroup, scheduled, dumper, mock(ProxyResourceManager.class));

        int time = rdbonlyRedisMasterReplication.commandTimeoutMilli();
        Assert.assertEquals(AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI, time);
    }

    @Test
    public void testProxiedMasterTimeoutMilli() throws CreateRdbDumperException {
        target = new ProxyEnabledEndpoint("localhost", randomPort(), new DefaultProxyConnectProtocol(null));
        this.keeperRedisMaster = new DefaultRedisMaster(keeperServer, target, eventLoopGroup, replicationStoreManager, scheduled, mock(ProxyResourceManager.class));
        keeperRedisMaster.setMasterState(MASTER_STATE.REDIS_REPL_CONNECTED);
        RedisMasterNewRdbDumper dumper = (RedisMasterNewRdbDumper)keeperRedisMaster.createRdbDumper();
        RdbonlyRedisMasterReplication rdbonlyRedisMasterReplication = new RdbonlyRedisMasterReplication(keeperServer,
                keeperRedisMaster, eventLoopGroup, scheduled, dumper, mock(ProxyResourceManager.class));

        int time = rdbonlyRedisMasterReplication.commandTimeoutMilli();
        Assert.assertEquals(AbstractRedisMasterReplication.PROXYED_REPLICATION_COMMAND_TIMEOUT_MILLI, time);
    }
}