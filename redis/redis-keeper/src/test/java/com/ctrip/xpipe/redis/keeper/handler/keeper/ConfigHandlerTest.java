package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
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

    private static final int CONFIG_GET_TIMEOUT_SECONDS = 30;

    private RedisKeeperServer keeperServer;

    @Before
    public void beforeConfigHandlerTest() throws Exception {
        // Avoid spurious LF lines before RDB that can desync downstream protocol parsers.
        fakeRedisServer.setSendLFBeforeSendRdb(false);
        keeperServer = startRedisKeeperServerAndConnectToFakeRedis();
        waitConditionUntilTimeOut(() -> keeperServer.getRedisMaster() != null
                && keeperServer.getRedisMaster().getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED);
    }

    @Test
    public void testConfigGetRordbSyncMasterSupport() throws Exception {
        SimpleObjectPool<NettyClient> clientPool = getXpipeNettyClientKeyedObjectPool()
                .getKeyPool(new DefaultEndPoint("127.0.0.1", keeperServer.getListeningPort()));
        ConfigGetCommand<Boolean> cfgGet = new ConfigGetCommand.ConfigGetRordbSync(clientPool, scheduled);
        fakeRedisServer.setSupportRordb(true);

        Assert.assertTrue(cfgGet.execute().get(CONFIG_GET_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    public void testConfigGetRordbSyncMasterNotSupport() throws Exception {
        SimpleObjectPool<NettyClient> clientPool = getXpipeNettyClientKeyedObjectPool()
                .getKeyPool(new DefaultEndPoint("127.0.0.1", keeperServer.getListeningPort()));
        ConfigGetCommand<Boolean> cfgGet = new ConfigGetCommand.ConfigGetRordbSync(clientPool, scheduled);
        fakeRedisServer.setSupportRordb(false);

        Assert.assertFalse(cfgGet.execute().get(CONFIG_GET_TIMEOUT_SECONDS, TimeUnit.SECONDS));
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

        Assert.assertFalse(cfgGet.execute().get(CONFIG_GET_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

}
