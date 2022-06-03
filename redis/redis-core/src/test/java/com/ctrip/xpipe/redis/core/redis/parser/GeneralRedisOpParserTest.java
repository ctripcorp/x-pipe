package com.ctrip.xpipe.redis.core.redis.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;

/**
 * @author lishanglin
 * date 2022/2/18
 */
@RunWith(SpringJUnit4ClassRunner.class)
public class GeneralRedisOpParserTest extends AbstractRedisOpParserTest {

    @Test
    public void testSetParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("SET", "k1", "v1").toArray());
        Assert.assertEquals(RedisOpType.SET, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("SET", "k1", "v1")), redisOp.buildRawOpArgs());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertArrayEquals("k1".getBytes(), redisSingleKeyOp.getKey().get());
        Assert.assertArrayEquals("v1".getBytes(), redisSingleKeyOp.getValue());
    }

    @Test
    public void testGtidParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("GTID", "a1:10", "set", "k1", "v1").toArray());
        Assert.assertEquals(RedisOpType.SET, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("GTID", "a1:10", "set", "k1", "v1")), redisOp.buildRawOpArgs());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertArrayEquals("k1".getBytes(), redisSingleKeyOp.getKey().get());
        Assert.assertArrayEquals("v1".getBytes(), redisSingleKeyOp.getValue());
    }

    @Test
    public void testMSetParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("MSET", "k1", "v1", "k2", "v2").toArray());
        Assert.assertEquals(RedisOpType.MSET, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("MSET", "k1", "v1", "k2", "v2")), redisOp.buildRawOpArgs());

        RedisMultiKeyOp redisMultiKeyOp = (RedisMultiKeyOp) redisOp;
        Assert.assertEquals(2, redisMultiKeyOp.getKeys().size());
        Assert.assertEquals(new RedisKey("k1"), redisMultiKeyOp.getKeyValue(0).getKey());
        Assert.assertArrayEquals("v1".getBytes(), redisMultiKeyOp.getKeyValue(0).getValue());
        Assert.assertEquals(new RedisKey("k2"), redisMultiKeyOp.getKeyValue(1).getKey());
        Assert.assertArrayEquals("v2".getBytes(), redisMultiKeyOp.getKeyValue(1).getValue());
    }

    @Test
    public void testGtidMSetParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("GTID", "a1:10", "MSET", "k1", "v1", "k2", "v2").toArray());
        Assert.assertEquals(RedisOpType.MSET, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("GTID", "a1:10", "MSET", "k1", "v1", "k2", "v2")), redisOp.buildRawOpArgs());

        RedisMultiKeyOp redisMultiKeyOp = (RedisMultiKeyOp) redisOp;
        Assert.assertEquals(2, redisMultiKeyOp.getKeys().size());
        Assert.assertEquals(new RedisKey("k1"), redisMultiKeyOp.getKeyValue(0).getKey());
        Assert.assertArrayEquals("v1".getBytes(), redisMultiKeyOp.getKeyValue(0).getValue());
        Assert.assertEquals(new RedisKey("k2"), redisMultiKeyOp.getKeyValue(1).getKey());
        Assert.assertArrayEquals("v2".getBytes(), redisMultiKeyOp.getKeyValue(1).getValue());
    }

    @Test
    public void testSelectParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("SELECT", "0").toArray());
        Assert.assertEquals(RedisOpType.SELECT, redisOp.getOpType());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertNull(redisSingleKeyOp.getKey());
        Assert.assertArrayEquals("0".getBytes(), redisSingleKeyOp.getValue());
    }

    @Test
    public void testPingParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("PING", "TEST").toArray());
        Assert.assertEquals(RedisOpType.PING, redisOp.getOpType());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertNull(redisSingleKeyOp.getKey());
        Assert.assertNull(redisSingleKeyOp.getValue());
    }

}
