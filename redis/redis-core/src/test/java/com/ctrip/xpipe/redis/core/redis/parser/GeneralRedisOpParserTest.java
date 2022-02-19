package com.ctrip.xpipe.redis.core.redis.parser;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.parser.*;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;

/**
 * @author lishanglin
 * date 2022/2/18
 */
@RunWith(SpringJUnit4ClassRunner.class)
public class GeneralRedisOpParserTest extends AbstractRedisTest {

    private RedisOpParserManager redisOpParserManager;

    private RedisOpParser parser;

    @Before
    public void setupGeneralRedisOpParserTest() {
        redisOpParserManager = new DefaultRedisOpParserManager();
        parser = new GeneralRedisOpParser(redisOpParserManager);
        new RedisOpSetParser(redisOpParserManager);
        new RedisOpMsetParser(redisOpParserManager);
        new RedisOpSelectParser(redisOpParserManager);
        new RedisOpPingParser(redisOpParserManager);
    }

    @Test
    public void testSetParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("SET", "k1", "v1"));
        Assert.assertEquals(RedisOpType.SET, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtidSet());
        Assert.assertEquals(Arrays.asList("SET", "k1", "v1"), redisOp.buildRawOpArgs());

        RedisSingleKeyOp<?> redisSingleKeyOp = (RedisSingleKeyOp<?>) redisOp;
        Assert.assertEquals("k1", redisSingleKeyOp.getKey().get());
        Assert.assertEquals("v1", redisSingleKeyOp.getValue());
    }

    @Test
    public void testGtidSetParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("GTID", "a1:10", "set", "k1", "v1"));
        Assert.assertEquals(RedisOpType.SET, redisOp.getOpType());
        Assert.assertEquals(new GtidSet("a1:10"), redisOp.getOpGtidSet());
        Assert.assertEquals(Arrays.asList("GTID", "a1:10", "set", "k1", "v1"), redisOp.buildRawOpArgs());

        RedisSingleKeyOp<?> redisSingleKeyOp = (RedisSingleKeyOp<?>) redisOp;
        Assert.assertEquals("k1", redisSingleKeyOp.getKey().get());
        Assert.assertEquals("v1", redisSingleKeyOp.getValue());
    }

    @Test
    public void testMSetParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("MSET", "k1", "v1", "k2", "v2"));
        Assert.assertEquals(RedisOpType.MSET, redisOp.getOpType());
        Assert.assertNull(redisOp.getOpGtidSet());
        Assert.assertEquals(Arrays.asList("MSET", "k1", "v1", "k2", "v2"), redisOp.buildRawOpArgs());

        RedisMultiKeyOp<?> redisMultiKeyOp = (RedisMultiKeyOp<?>) redisOp;
        Assert.assertEquals(2, redisMultiKeyOp.getKeys().size());
        Assert.assertEquals(Pair.of(new RedisKey("k1"), "v1"), redisMultiKeyOp.getKeyValue(0));
        Assert.assertEquals(Pair.of(new RedisKey("k2"), "v2"), redisMultiKeyOp.getKeyValue(1));
    }

    @Test
    public void testGtidMSetParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("GTID", "a1:10", "MSET", "k1", "v1", "k2", "v2"));
        Assert.assertEquals(RedisOpType.MSET, redisOp.getOpType());
        Assert.assertEquals(new GtidSet("a1:10"), redisOp.getOpGtidSet());
        Assert.assertEquals(Arrays.asList("GTID", "a1:10", "MSET", "k1", "v1", "k2", "v2"), redisOp.buildRawOpArgs());

        RedisMultiKeyOp<?> redisMultiKeyOp = (RedisMultiKeyOp<?>) redisOp;
        Assert.assertEquals(2, redisMultiKeyOp.getKeys().size());
        Assert.assertEquals(Pair.of(new RedisKey("k1"), "v1"), redisMultiKeyOp.getKeyValue(0));
        Assert.assertEquals(Pair.of(new RedisKey("k2"), "v2"), redisMultiKeyOp.getKeyValue(1));
    }

    @Test
    public void testSelectParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("SELECT", "0"));
        Assert.assertEquals(RedisOpType.SELECT, redisOp.getOpType());

        RedisSingleKeyOp<?> redisSingleKeyOp = (RedisSingleKeyOp<?>) redisOp;
        Assert.assertNull(redisSingleKeyOp.getKey());
        Assert.assertEquals(0L, redisSingleKeyOp.getValue());
    }

    @Test
    public void testPingParse() {
        RedisOp redisOp = parser.parse(Arrays.asList("PING", "TEST"));
        Assert.assertEquals(RedisOpType.PING, redisOp.getOpType());

        RedisSingleKeyOp<?> redisSingleKeyOp = (RedisSingleKeyOp<?>) redisOp;
        Assert.assertNull(redisSingleKeyOp.getKey());
        Assert.assertNull(redisSingleKeyOp.getValue());
    }

}
