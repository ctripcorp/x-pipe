package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.ctrip.xpipe.redis.core.redis.rdb.parser.RdbDataBytes.hllKeys;
import static com.ctrip.xpipe.redis.core.redis.rdb.parser.RdbDataBytes.hllPayloads;
import static com.ctrip.xpipe.redis.core.redis.rdb.parser.RdbDataBytes.hllRdbBytes;

public class HyperLogLogRdbParserTest extends AbstractTest implements RdbParseListener {

    private DefaultRdbParser parser;
    private DefaultRdbParseContext context;
    private List<RedisOp> redisOps;

    @Before
    public void setUp() {
        context = new DefaultRdbParseContext();
        parser = new DefaultRdbParser(context);
        parser.registerListener(this);
        redisOps = new ArrayList<>();
    }

    @Test
    public void testParseRedis820HyperLogLogRdbWithoutPayloadLoss() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(hllRdbBytes);
        while (!parser.isFinish()) {
            parser.read(byteBuf);
        }

        Assert.assertEquals(12, context.getRdbVersion());
        Assert.assertEquals(hllKeys.length + 1, redisOps.size());
        Assert.assertEquals(RedisOpType.SELECT, redisOps.get(0).getOpType());

        for (int i = 0; i < hllKeys.length; i++) {
            RedisOp hllSet = redisOps.get(i + 1);
            Assert.assertEquals(RedisOpType.SET, hllSet.getOpType());
            Assert.assertArrayEquals(hllKeys[i].getBytes(StandardCharsets.US_ASCII), hllSet.buildRawOpArgs()[1]);
            Assert.assertArrayEquals(hllPayloads[i], hllSet.buildRawOpArgs()[2]);
            Assert.assertArrayEquals("HYLL".getBytes(StandardCharsets.US_ASCII),
                    Arrays.copyOf(hllSet.buildRawOpArgs()[2], 4));
        }
    }

    @Override
    public void onRedisOp(RedisOp redisOp) {
        redisOps.add(redisOp);
    }

    @Override
    public void onAux(String key, String value) {
    }

    @Override
    public void onFinish(RdbParser<?> parser) {
    }

    @Override
    public void onAuxFinish(Map<String, String> auxMap) {
    }
}
