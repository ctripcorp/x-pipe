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


    @Test
    public void testParseFromDirectByteBufInOutPayloadZAdd() throws IOException {
        DirectByteBufInOutPayload gtid = new DirectByteBufInOutPayload();
        gtid.startInput();
        gtid.in(Unpooled.wrappedBuffer("GTID".getBytes()));

        DirectByteBufInOutPayload gtidStr = new DirectByteBufInOutPayload();
        gtidStr.startInput();
        gtidStr.in(Unpooled.wrappedBuffer("573c4305037d8c0cddb1ee36b5455d3a9d36f43e:1268595202".getBytes()));

        DirectByteBufInOutPayload db = new DirectByteBufInOutPayload();
        db.startInput();
        db.in(Unpooled.wrappedBuffer("0".getBytes()));


        DirectByteBufInOutPayload command = new DirectByteBufInOutPayload();
        command.startInput();
        command.in(Unpooled.wrappedBuffer("zadd".getBytes()));
        DirectByteBufInOutPayload key = new DirectByteBufInOutPayload();
        key.startInput();
        key.in(Unpooled.wrappedBuffer("zset:did2vid:1027cff212d9d9a6".getBytes()));
        DirectByteBufInOutPayload nx = new DirectByteBufInOutPayload();
        nx.startInput();
        nx.in(Unpooled.wrappedBuffer("nx".getBytes()));
        DirectByteBufInOutPayload score = new DirectByteBufInOutPayload();
        score.startInput();
        score.in(Unpooled.wrappedBuffer("1.784010284E9".getBytes()));
        DirectByteBufInOutPayload value = new DirectByteBufInOutPayload();
        value.startInput();
        value.in(Unpooled.wrappedBuffer("09C74910607911F083DBFFC139EB4BA5".getBytes()));

        RedisOpItem item = RedisOpItemParser.parse(parser, new Object[]{gtid,gtidStr,db,command, key,nx,score, value});

        Assert.assertEquals(RedisOpType.ZADD, item.getRedisOpType());
        Assert.assertNotNull(item.getRedisKey());
        Assert.assertArrayEquals("zset:did2vid:1027cff212d9d9a6".getBytes(), item.getRedisKey().get());
    }
}
