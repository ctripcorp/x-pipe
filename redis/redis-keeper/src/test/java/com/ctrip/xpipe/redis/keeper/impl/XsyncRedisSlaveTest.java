package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisSingleKeyOpGtidWrapper;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 * date 2023/11/10
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class XsyncRedisSlaveTest extends AbstractRedisKeeperTest {

    @Mock
    public Channel channel;

    @Mock
    public RedisKeeperServer redisKeeperServer;

    @Mock
    private KeeperRepl keeperRepl;

    private XsyncRedisSlave redisSlave;

    @Before
    public void setupXsyncRedisSlaveTest() {
        when(channel.closeFuture()).thenReturn(new DefaultChannelPromise(channel));
        when(channel.remoteAddress()).thenReturn(localhostInetAdress(randomPort()));
        when(keeperRepl.replId()).thenReturn("test-repl-id");
        when(redisKeeperServer.getKeeperRepl()).thenReturn(keeperRepl);

        RedisClient redisClient = new DefaultRedisClient(channel, redisKeeperServer);
        redisSlave= new XsyncRedisSlave(redisClient);
    }

    @Test
    public void testFilterPublish() {
        RedisSingleKeyOp op1 = new RedisOpSingleKey(RedisOpType.SET, string2Bytes("set a 1"), null, null);
        RedisOp gtidOp1 = new RedisSingleKeyOpGtidWrapper(string2Bytes("GTID ggg:1 0"), "ggg", op1);
        Assert.assertFalse(redisSlave.shouldFilter(gtidOp1));

        RedisSingleKeyOp op2 = new RedisOpSingleKey(RedisOpType.PUBLISH, string2Bytes("publish xpipe-asymmetric-ppp 222"), null, null);
        RedisOp gtidOp2 = new RedisSingleKeyOpGtidWrapper(string2Bytes("GTID ggg:1 0"), "ggg", op2);
        Assert.assertFalse(redisSlave.shouldFilter(gtidOp2));

        RedisSingleKeyOp op3 = new RedisOpSingleKey(RedisOpType.PUBLISH, string2Bytes("publish ppp 222"), null, null);
        RedisOp gtidOp3 = new RedisSingleKeyOpGtidWrapper(string2Bytes("GTID ggg:1 0"), "ggg", op3);
        Assert.assertTrue(redisSlave.shouldFilter(gtidOp3));
        Assert.assertTrue(redisSlave.shouldFilter(op3));

        //test estimated size by the way
        Assert.assertEquals(15, gtidOp1.estimatedSize());
        Assert.assertEquals(40, gtidOp2.estimatedSize());
        Assert.assertEquals(23, gtidOp3.estimatedSize());
    }

    private byte[][] string2Bytes(String s) {

        String[] ss = s.split(" ");
        int length = ss.length;
        byte[][] b = new byte[length][];

        for (int i = 0; i < length; i++) {
            b[i] = ss[i].getBytes();
        }

        return b;
    }

}
