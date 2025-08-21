package com.ctrip.xpipe.redis.core.redis.op;

import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author lishanglin
 * date 2022/2/22
 */
public class RedisOpDelTest extends AbstractRedisOpParserTest {

    @Test
    public void testDivideDelOp() {
        RedisOp redisOp = parser.parse(Arrays.asList("del", "k1", "k2", "k3", "k4", "k5").toArray());
        Assert.assertEquals(RedisOpType.DEL, redisOp.getOpType());
        RedisMultiKeyOp redisOpDel = (RedisMultiKeyOp) redisOp;

        RedisMultiKeyOp subOp1 = redisOpDel.subOp(Sets.newHashSet(0, 2, 4));
        RedisMultiKeyOp subOp2 = redisOpDel.subOp(Sets.newHashSet(1, 3));

        Assert.assertEquals(RedisOpType.DEL, subOp1.getOpType());
        Assert.assertEquals(RedisOpType.DEL, subOp2.getOpType());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("del", "k1", "k3", "k5")), subOp1.buildRawOpArgs());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("del", "k2", "k4")), subOp2.buildRawOpArgs());
    }

    @Test
    public void testDivideGtidDelOp() {
        RedisOp redisOp = parser.parse(Arrays.asList("gtid", "a1:100", "0", "del", "k1", "k2", "k3", "k4", "k5").toArray());
        Assert.assertEquals(RedisOpType.DEL, redisOp.getOpType());
        RedisMultiKeyOp redisOpDel = (RedisMultiKeyOp) redisOp;

        RedisMultiKeyOp subOp1 = redisOpDel.subOp(Sets.newHashSet(0, 2, 4));
        RedisMultiKeyOp subOp2 = redisOpDel.subOp(Sets.newHashSet(1, 3));

        Assert.assertEquals(RedisOpType.DEL, subOp1.getOpType());
        Assert.assertEquals(RedisOpType.DEL, subOp2.getOpType());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("gtid", "a1:100", "0", "del", "k1", "k3", "k5")), subOp1.buildRawOpArgs());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("gtid", "a1:100", "0", "del", "k2", "k4")), subOp2.buildRawOpArgs());
    }

}
