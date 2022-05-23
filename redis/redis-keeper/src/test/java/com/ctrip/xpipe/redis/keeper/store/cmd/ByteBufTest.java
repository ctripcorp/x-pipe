package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.AbstractTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author lishanglin
 * date 2022/5/23
 */
public class ByteBufTest extends AbstractTest {

    @Test
    public void testReadIndex() {
        ByteBuf byteBuf = Unpooled.directBuffer(1024);
        Assert.assertEquals(0, byteBuf.readableBytes());
        Assert.assertEquals(0, byteBuf.readerIndex());

        for (int i = 0; i < 512; i++) {
            byteBuf.writeByte('a' + (int)(26 * Math.random()));
        }

        Assert.assertEquals(512, byteBuf.readableBytes());
        Assert.assertEquals(0, byteBuf.readerIndex());

        for (int i = 0; i < 512; i++) {
            byteBuf.readByte();
        }
        Assert.assertEquals(0, byteBuf.readableBytes());
        Assert.assertEquals(512, byteBuf.readerIndex());

        byteBuf.readerIndex(0);
        Assert.assertEquals(512, byteBuf.readableBytes());
        Assert.assertEquals(0, byteBuf.readerIndex());
    }

}
