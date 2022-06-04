package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.codec.Codec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author lishanglin
 * date 2022/6/4
 */
public class RdbStringParserTest extends AbstractTest {

    private RdbStringParser rdbStringParser;

    // k1 v1
    private byte[] rdbBytesPlainStr = new byte[] {0x02, 0x6b, 0x31, 0x02, 0x76, 0x31};
    // k4 100
    private byte[] rdbBytesIntAsStr = new byte[] {0x02, 0x6b, 0x34, (byte)0xc0, 0x64};
    // k3 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    private byte[] rdbBytesLzfStr = new byte[] {0x02, 0x6b, 0x33, (byte)0xc3, 0x09, 0x3a, 0x01, 0x61, 0x61, (byte)0xe0, 0x2d, 0x00, 0x01, 0x61, 0x61};

    @Before
    public void setupRdbStringParserTest() {
        this.rdbStringParser = new RdbStringParser();
    }

    @Test
    public void testParsePlainStr() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(rdbBytesPlainStr);
        Assert.assertEquals("k1", readStr(byteBuf));
        Assert.assertEquals("v1", readStr(byteBuf));
    }

    @Test
    public void testParseIntAsStr() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(rdbBytesIntAsStr);
        Assert.assertEquals("k4", readStr(byteBuf));
        Assert.assertEquals("100", readStr(byteBuf));
    }

    @Test
    public void testParseLzfStr() {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(rdbBytesLzfStr);
        Assert.assertEquals("k3", readStr(byteBuf));
        Assert.assertEquals("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", readStr(byteBuf));
    }

    private String readStr(ByteBuf src) {
        byte[] rst = null;
        while(null == rst) {
            rst = rdbStringParser.read(src);
        }

        rdbStringParser.reset();
        return new String(rst, Codec.defaultCharset);
    }

}
