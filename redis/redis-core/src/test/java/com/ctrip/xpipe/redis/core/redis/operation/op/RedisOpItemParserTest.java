package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.payload.DirectByteBufInOutPayload;
import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class RedisOpItemParserTest extends AbstractRedisOpParserTest {

    @Test
    public void testParseFromDirectByteBufInOutPayload() throws IOException {
        DirectByteBufInOutPayload command = new DirectByteBufInOutPayload();
        command.startInput();
        command.in(Unpooled.wrappedBuffer("SET".getBytes()));
        DirectByteBufInOutPayload key = new DirectByteBufInOutPayload();
        key.startInput();
        key.in(Unpooled.wrappedBuffer("foo".getBytes()));
        DirectByteBufInOutPayload value = new DirectByteBufInOutPayload();
        value.startInput();
        value.in(Unpooled.wrappedBuffer("bar".getBytes()));

        RedisOpItem item = RedisOpItemParser.parse(parser, new Object[]{command, key, value});

        Assert.assertEquals(RedisOpType.SET, item.getRedisOpType());
        Assert.assertNotNull(item.getRedisKey());
        Assert.assertArrayEquals("foo".getBytes(), item.getRedisKey().get());
    }
}
