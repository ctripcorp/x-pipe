package com.ctrip.xpipe.redis.core.redis.op;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpDel;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.parser.RedisOpDelParser;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author lishanglin
 * date 2022/2/22
 */
public class RedisOpDelTest extends AbstractTest {

    private RedisOpParserManager manager;

    private RedisOpParser parser;

    @Before
    public void setupRedisOpDelTest() {
        manager = new DefaultRedisOpParserManager();
        parser = new GeneralRedisOpParser(manager);
        new RedisOpDelParser(manager);
    }

    @Test
    public void testDivideDelOp() {
        RedisOp redisOp = parser.parse(Arrays.asList("del", "k1", "k2", "k3", "k4", "k5"));
        Assert.assertEquals(RedisOpType.DEL, redisOp.getOpType());
        RedisOpDel redisOpDel = (RedisOpDel) redisOp;

        RedisOpDel subOp1 = (RedisOpDel) redisOpDel.subOp(Sets.newHashSet(0, 2, 4));
        RedisOpDel subOp2 = (RedisOpDel) redisOpDel.subOp(Sets.newHashSet(1, 3));

        Assert.assertEquals(RedisOpType.DEL, subOp1.getOpType());
        Assert.assertEquals(RedisOpType.DEL, subOp2.getOpType());
        Assert.assertEquals(Arrays.asList("DEL", "k1", "k3", "k5"), subOp1.buildRawOpArgs());
        Assert.assertEquals(Arrays.asList("DEL", "k2", "k4"), subOp2.buildRawOpArgs());
    }

    @Test
    public void testDivideGtidDelOp() {
        RedisOp redisOp = parser.parse(Arrays.asList("gtid", "a1:100", "del", "k1", "k2", "k3", "k4", "k5"));
        Assert.assertEquals(RedisOpType.DEL, redisOp.getOpType());
        RedisMultiKeyOp<?> redisOpDel = (RedisMultiKeyOp<?>) redisOp;

        RedisMultiKeyOp<?> subOp1 = redisOpDel.subOp(Sets.newHashSet(0, 2, 4));
        RedisMultiKeyOp<?> subOp2 = redisOpDel.subOp(Sets.newHashSet(1, 3));

        Assert.assertEquals(RedisOpType.DEL, subOp1.getOpType());
        Assert.assertEquals(RedisOpType.DEL, subOp2.getOpType());
        Assert.assertEquals(Arrays.asList("GTID", "a1:100", "DEL", "k1", "k3", "k5"), subOp1.buildRawOpArgs());
        Assert.assertEquals(Arrays.asList("GTID", "a1:100", "DEL", "k2", "k4"), subOp2.buildRawOpArgs());
    }

}
