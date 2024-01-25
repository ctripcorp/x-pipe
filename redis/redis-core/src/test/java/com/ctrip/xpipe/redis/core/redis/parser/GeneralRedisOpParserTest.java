package com.ctrip.xpipe.redis.core.redis.parser;

import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpLwm;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpMergeEnd;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpMergeStart;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/18
 */
@RunWith(SpringJUnit4ClassRunner.class)
public class GeneralRedisOpParserTest extends AbstractRedisOpParserTest {

    @Test
    public void testCtripMergeStartParse() {
        RedisOpMergeStart redisOpMergeStart = new RedisOpMergeStart();
        RedisOp redisOp = parser.parse(redisOpMergeStart.buildRawOpArgs());
        Assert.assertEquals(RedisOpType.CTRIP_MERGE_START, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());
        Assert.assertArrayEquals(redisOpMergeStart.buildRawOpArgs(), redisOp.buildRawOpArgs());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertNull(redisSingleKeyOp.getKey());
        Assert.assertNull(redisSingleKeyOp.getValue());
        Assert.assertTrue(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testCtripMergeEndParse() {
        RedisOpMergeEnd redisOpMergeEnd = new RedisOpMergeEnd("24d9e2513182d156cbd999df5ebedf24e7634140:1-1494763841");
        RedisOp redisOp = parser.parse(redisOpMergeEnd.buildRawOpArgs());
        Assert.assertEquals(RedisOpType.CTRIP_MERGE_END, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());
        Assert.assertArrayEquals(redisOpMergeEnd.buildRawOpArgs(), redisOp.buildRawOpArgs());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertArrayEquals("24d9e2513182d156cbd999df5ebedf24e7634140:1-1494763841".getBytes(), redisSingleKeyOp.getKey().get());
        Assert.assertNull(redisSingleKeyOp.getValue());
        Assert.assertTrue(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testCtripGtidLwmParse() {
        RedisOpLwm redisOpLwm = new RedisOpLwm("24d9e2513182d156cbd999df5ebedf24e7634140", 1494763841L);
        RedisOp redisOp = parser.parse(redisOpLwm.buildRawOpArgs());
        Assert.assertEquals(RedisOpType.GTID_LWM, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());
        Assert.assertArrayEquals(redisOpLwm.buildRawOpArgs(), redisOp.buildRawOpArgs());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertArrayEquals("24d9e2513182d156cbd999df5ebedf24e7634140".getBytes(), redisSingleKeyOp.getKey().get());
        Assert.assertArrayEquals("1494763841".getBytes(), redisSingleKeyOp.getValue());
        Assert.assertFalse(redisOp.getOpType().isSwallow());
    }
    @Test
    public void testUnknowParse() {
        byte[][] rawOpArgs = {"unknow".getBytes(), "unknow_key".getBytes(), "unknow_value".getBytes()};
        RedisOp redisOp = parser.parse(rawOpArgs);
        Assert.assertEquals(RedisOpType.UNKNOWN, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());
        Assert.assertArrayEquals(rawOpArgs, redisOp.buildRawOpArgs());
        byte[][] rawOpArgs1 = {"credis_flushall".getBytes()};
        redisOp = parser.parse(rawOpArgs1);
        Assert.assertEquals(RedisOpType.UNKNOWN, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());
        Assert.assertArrayEquals(rawOpArgs1, redisOp.buildRawOpArgs());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertNull(redisSingleKeyOp.getKey());
        Assert.assertNull(redisSingleKeyOp.getValue());
        Assert.assertTrue(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testSetParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("SET", "k1", "v1").toArray());
        Assert.assertEquals(RedisOpType.SET, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtid());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("SET", "k1", "v1")), redisOp.buildRawOpArgs());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertArrayEquals("k1".getBytes(), redisSingleKeyOp.getKey().get());
        Assert.assertArrayEquals("v1".getBytes(), redisSingleKeyOp.getValue());
        Assert.assertFalse(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testGtidParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("GTID", "a1:10", "0", "set", "k1", "v1").toArray());
        Assert.assertEquals(RedisOpType.SET, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("GTID", "a1:10", "0", "set", "k1", "v1")), redisOp.buildRawOpArgs());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertArrayEquals("k1".getBytes(), redisSingleKeyOp.getKey().get());
        Assert.assertArrayEquals("v1".getBytes(), redisSingleKeyOp.getValue());
        Assert.assertFalse(redisOp.getOpType().isSwallow());
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
        Assert.assertFalse(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testGtidMSetParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("GTID", "a1:10", "0", "MSET", "k1", "v1", "k2", "v2").toArray());
        Assert.assertEquals(RedisOpType.MSET, redisOp.getOpType());
        Assert.assertEquals("a1:10", redisOp.getOpGtid());
        Assert.assertArrayEquals(strList2bytesArray(Arrays.asList("GTID", "a1:10", "0", "MSET", "k1", "v1", "k2", "v2")), redisOp.buildRawOpArgs());

        RedisMultiKeyOp redisMultiKeyOp = (RedisMultiKeyOp) redisOp;
        Assert.assertEquals(2, redisMultiKeyOp.getKeys().size());
        Assert.assertEquals(new RedisKey("k1"), redisMultiKeyOp.getKeyValue(0).getKey());
        Assert.assertArrayEquals("v1".getBytes(), redisMultiKeyOp.getKeyValue(0).getValue());
        Assert.assertEquals(new RedisKey("k2"), redisMultiKeyOp.getKeyValue(1).getKey());
        Assert.assertArrayEquals("v2".getBytes(), redisMultiKeyOp.getKeyValue(1).getValue());
        Assert.assertFalse(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testSelectParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("SELECT", "0").toArray());
        Assert.assertEquals(RedisOpType.SELECT, redisOp.getOpType());
        Assert.assertFalse(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testPingParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("PING", "TEST").toArray());
        Assert.assertEquals(RedisOpType.PING, redisOp.getOpType());

        RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
        Assert.assertNull(redisSingleKeyOp.getKey());
        Assert.assertNull(redisSingleKeyOp.getValue());
        Assert.assertFalse(redisOp.getOpType().isSwallow());
    }

    @Test
    public void testNoneExistsCmdParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("EMPTY", "0").toArray());
        Assert.assertEquals(RedisOpType.UNKNOWN, redisOp.getOpType());
        Assert.assertTrue(redisOp.getOpType().isSwallow());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParamShorterParse() {
        parser.parse(Arrays.asList("SET").toArray());
    }

    @Test
    public void testParamLongerParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("SET", "a", "1", "b").toArray());
        Assert.assertEquals(RedisOpType.SET, redisOp.getOpType());
    }

    @Test
    public void testParseAllCmds() {
        List<String> cmdList = Arrays.asList(
                "append",
                "decr",
                "decrby",
                "del",
                "expire",
                "expireat",
                "geoadd",
                "georadius",
                "getset",
                "hdel",
                "hincrby",
                "hincrbyfloat",
                "hmset",
                "hset",
                "hsetnx",
                "incr",
                "incrby",
                "linsert",
                "lpop",
                "lpush",
                "lpushx",
                "lrem",
                "lset",
                "ltrim",
                "move",
                "mset",
                "msetnx",
                "persist",
                "pexpire",
                "pexpireat",
                "psetex",
                "rpop",
                "rpush",
                "rpushx",
                "sadd",
                "set",
                "setbit",
                "setex",
                "setnx",
                "setrange",
                "spop",
                "srem",
                "unlink",
                "zadd",
                "zincrby",
                "zrem",
                "zremrangebylex",
                "zremrangebyrank",
                "zremrangebyscore",
                "incrbyfloat",
                "publish",
                "ping",
                "select",
                "exec",
                "script",
                "multi"
        );

        for (String cmd : cmdList) {
            RedisOpType redisOpType = RedisOpType.lookup(cmd);
            List<String> parserList = new ArrayList<>();
            System.out.println(cmd);
            parserList.add(cmd);
            for (int i = 0; i < Math.abs(redisOpType.getArity()) - 1; i++) {
                parserList.add("0");
            }
            parser.parse(parserList.toArray());
        }
    }
}
