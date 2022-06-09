package com.ctrip.xpipe.redis.core.redis.op;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author lishanglin
 * date 2022/2/22
 */
public class RedisOpMsetTest extends AbstractRedisOpParserTest {

    @Test
    public void testDivideMsetTest() {
        RedisOp redisOp = parser.parse(Arrays.asList("mset", "k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4", "k5", "v5").toArray());
        Assert.assertEquals(RedisOpType.MSET, redisOp.getOpType());

        RedisMultiKeyOp subOp1 = ((RedisMultiKeyOp) redisOp).subOp(Sets.newHashSet(0, 2, 4));
        RedisMultiKeyOp subOp2 = ((RedisMultiKeyOp) redisOp).subOp(Sets.newHashSet(1, 3));
        Assert.assertEquals(RedisOpType.MSET, subOp1.getOpType());
        Assert.assertEquals(RedisOpType.MSET, subOp2.getOpType());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("mset", "k1", "v1", "k3", "v3", "k5", "v5")), subOp1.buildRawOpArgs());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("mset", "k2", "v2", "k4", "v4")), subOp2.buildRawOpArgs());
    }

    @Test
    public void testDivideGtidMsetTest() {
        RedisOp redisOp = parser.parse(Arrays.asList("gtid", "a1:100", "mset", "k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4", "k5", "v5").toArray());
        Assert.assertEquals(RedisOpType.MSET, redisOp.getOpType());

        RedisMultiKeyOp subOp1 = ((RedisMultiKeyOp) redisOp).subOp(Sets.newHashSet(0, 2, 4));
        RedisMultiKeyOp subOp2 = ((RedisMultiKeyOp) redisOp).subOp(Sets.newHashSet(1, 3));
        Assert.assertEquals(RedisOpType.MSET, subOp1.getOpType());
        Assert.assertEquals(RedisOpType.MSET, subOp2.getOpType());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("gtid", "a1:100", "mset", "k1", "v1", "k3", "v3", "k5", "v5")), subOp1.buildRawOpArgs());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("gtid", "a1:100", "mset", "k2", "v2", "k4", "v4")), subOp2.buildRawOpArgs());
    }

}
