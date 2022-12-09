package com.ctrip.xpipe.redis.core.redis.rdb.encoding;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.DefaultRdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.RdbStringParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author lishanglin
 * date 2022/6/22
 */
public class StreamListpackTest extends AbstractTest {

    private RdbStringParser rdbStringParser;

    // master ID + listpack
    private static final byte[] rdbStreamListpackBytes = new byte[] {0x10, 0x00, 0x00, 0x01, (byte)0x81, (byte)0x8a,
            (byte)0x8c, (byte)0xf1, (byte)0x91, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte)0xc3, 0x40, 0x7b,
            0x40, (byte)0xaf, 0x14, (byte)0xaf, 0x00, 0x00, 0x00, 0x24, 0x00, 0x03, 0x01, 0x01, 0x01, 0x02, 0x01,
            (byte)0x82, 0x6b, 0x31, 0x03, (byte)0x82, 0x6b, 0x32, 0x03, 0x00, 0x20, 0x0b, 0x00, 0x00, 0x20, 0x01, 0x01,
            (byte)0x82, 0x76, 0x20, 0x0f, 0x09, 0x76, 0x32, 0x03, 0x05, 0x01, 0x03, 0x01, (byte)0xf1, 0x75, 0x2e, 0x20,
            0x17, 0x02, (byte)0x82, 0x76, 0x33, 0x20, 0x11, 0x00, 0x34, 0x20, 0x11, 0x07, 0x00, 0x01, (byte)0xf2, 0x30,
            0x06, 0x01, 0x04, 0x00, 0x20, 0x1a, 0x1e, (byte)0x82, 0x6b, 0x33, 0x03, (byte)0xf2, (byte)0xff, (byte)0xff,
            0x00, 0x04, (byte)0x82, 0x6b, 0x34, 0x03, (byte)0xd0, 0x00, 0x02, (byte)0x82, 0x6b, 0x35, 0x03, (byte)0xf4,
            (byte)0xff, 0x7f, (byte)0xc6, (byte)0xa4, 0x7e, (byte)0x8d, 0x03, 0x00, 0x09, 0x0a, 0x40, 0x2a, 0x01, 0x58,
            0x5d, 0x40, 0x2a, 0x00, 0x01, 0x20, 0x2a, 0x03, 0x36, 0x03, (byte)0xb8, 0x61, (byte)0xe0, 0x2e, 0x00, 0x03,
            0x39, 0x06, 0x01, (byte)0xff};

    @Before
    public void setupStreamListpackTest() {
        this.rdbStringParser = new RdbStringParser(new DefaultRdbParseContext());
    }

    @Test
    public void testStreamListpackParse() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(rdbStreamListpackBytes);
        byte[] masterIdStr = rdbStringParser.read(byteBuf);
        rdbStringParser.reset();

        StreamID masterId = new StreamID(masterIdStr);
        logger.info("[master ID] {}", masterId);

        byte[] listpackStr = rdbStringParser.read(byteBuf);
        rdbStringParser.reset();

        Listpack listpack = new Listpack(listpackStr);
        for (byte[] val: listpack.convertToList()) {
            logger.info("[listpackParseTest] {}", new String(val));
        }

        StreamListpackIterator iterator = new StreamListpackIterator(masterId, listpack);
        RedisKey redisKey = new RedisKey("stream");
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("XADD stream 1655886901649-0 k1 v1 k2 v2", iterator.next().buildRedisOp(redisKey).toString());
        Assert.assertEquals("XDEL stream 1655886913542-0", iterator.next().buildRedisOp(redisKey).toString());
        Assert.assertEquals("XADD stream 1655886968769-0 k3 65535 k4 -4096 k5 999999999999999", iterator.next().buildRedisOp(redisKey).toString());
        Assert.assertEquals("XADD stream 1655886991081-0 k6 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", iterator.next().buildRedisOp(redisKey).toString());
        Assert.assertFalse(iterator.hasNext());
    }

}
