package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigGetCommand;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @author lishanglin
 * date 2024/3/5
 */
public class ConfigHandlerTest extends AbstractFakeRedisTest {

    private RedisKeeperServer keeperServer;

    @Before
    public void beforeConfigHandlerTest() throws Exception {
        keeperServer = startRedisKeeperServerAndConnectToFakeRedis();
    }

    @Test
    public void testConfigGetRordbSyncMasterSupport() throws Exception {
        SimpleObjectPool<NettyClient> clientPool = getXpipeNettyClientKeyedObjectPool()
                .getKeyPool(new DefaultEndPoint("127.0.0.1", keeperServer.getListeningPort()));
        ConfigGetCommand<Boolean> cfgGet = new ConfigGetCommand.ConfigGetRordbSync(clientPool, scheduled);
        fakeRedisServer.setSupportRordb(true);

        Assert.assertTrue(cfgGet.execute().get(1, TimeUnit.SECONDS));
    }

    @Test
    public void testConfigGetRordbSyncMasterNotSupport() throws Exception {
        SimpleObjectPool<NettyClient> clientPool = getXpipeNettyClientKeyedObjectPool()
                .getKeyPool(new DefaultEndPoint("127.0.0.1", keeperServer.getListeningPort()));
        ConfigGetCommand<Boolean> cfgGet = new ConfigGetCommand.ConfigGetRordbSync(clientPool, scheduled);
        fakeRedisServer.setSupportRordb(false);

        Assert.assertFalse(cfgGet.execute().get(1, TimeUnit.SECONDS));
    }

    @Test
    public void testConfigGetUnknown() throws Exception {
        SimpleObjectPool<NettyClient> clientPool = getXpipeNettyClientKeyedObjectPool()
                .getKeyPool(new DefaultEndPoint("127.0.0.1", keeperServer.getListeningPort()));
        ConfigGetCommand<Boolean> cfgGet = new ConfigGetCommand.ConfigGetBool(clientPool, scheduled) {
            @Override
            protected String getConfigName() {
                return "mockconfig";
            }

            @Override
            protected Boolean defaultValue() {
                return false;
            }
        };

        Assert.assertFalse(cfgGet.execute().get(1, TimeUnit.SECONDS));
    }

}
