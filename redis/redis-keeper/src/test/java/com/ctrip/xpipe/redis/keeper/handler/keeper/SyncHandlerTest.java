package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;


/**
 * @author TB
 * @date 2026/7/24 15:00
 */

@RunWith(MockitoJUnitRunner.class)
public class SyncHandlerTest extends AbstractFakeRedisTest {
    private CommandHandlerManager commandHandlerManager;

    @Mock
    private RedisClient redisClient;

    private RedisKeeperServer redisKeeperServer;

    @Before
    public void setUp() throws Exception {
        commandHandlerManager = new CommandHandlerManager();
        redisKeeperServer = startRedisKeeperServerAndConnectToFakeRedis();
        redisKeeperServer = Mockito.spy(redisKeeperServer);
        waitConditionUntilTimeOut(() -> redisKeeperServer.getRedisMaster() != null
                && redisKeeperServer.getRedisMaster().getMasterState() == MASTER_STATE.REDIS_REPL_CONNECTED);
        Mockito.doReturn(redisKeeperServer).when(redisClient).getRedisServer();
    }

    @Test
    public void testRedisClientClose() throws Exception {
        commandHandlerManager.handle(new String[] {"sync"},redisClient);
        sleep(1000);
        Mockito.verify(redisClient,Mockito.times(1)).close();
    }

    @Test
    public void testRedisServerDisposedRedisClientConnect() throws Exception {
        Mockito.doThrow(new RedisKeeperServerStateException("redisKeeperServer","disposed")).when(redisKeeperServer).getReplicationStore();
        commandHandlerManager.handle(new String[] {"psync"},redisClient);
        sleep(1000);
        Mockito.verify(redisClient,Mockito.times(1)).close();
    }


}
