package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateActive;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateBackup;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 21, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class SlaveOfCommandHandlerTest extends AbstractRedisKeeperTest{

    @Mock
    private RedisClient redisClient;

    @Mock
    private RedisKeeperServer redisKeeperServer;

    private SlaveOfCommandHandler slaveOfCommandHandler;

    @Before
    public void beforeSlaveOfCommandHandlerTest(){

        when(redisClient.getRedisServer()).thenReturn(redisKeeperServer);
        slaveOfCommandHandler = new SlaveOfCommandHandler();

    }



    @Test
    public void testSlaveOf(){

        String []args = new String[]{"127.0.0.1", "0"};

        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateActive(redisKeeperServer));
        slaveOfCommandHandler.doHandle(args, redisClient);
        verify(redisKeeperServer).reconnectMaster();

        reset(redisKeeperServer);

        when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(new RedisKeeperServerStateBackup(redisKeeperServer));
        slaveOfCommandHandler.doHandle(args, redisClient);
        verify(redisKeeperServer, never()).reconnectMaster();

    }
}
