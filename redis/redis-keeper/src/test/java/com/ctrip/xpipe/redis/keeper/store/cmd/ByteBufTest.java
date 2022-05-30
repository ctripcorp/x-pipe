package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.AbstractTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
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

        writeRandomStrToByteBuf(byteBuf, 512);

        Assert.assertEquals(512, byteBuf.readableBytes());
        Assert.assertEquals(0, byteBuf.readerIndex());

        readByteBuf(byteBuf, 512);
        Assert.assertEquals(0, byteBuf.readableBytes());
        Assert.assertEquals(512, byteBuf.readerIndex());

        byteBuf.readerIndex(0);
        Assert.assertEquals(512, byteBuf.readableBytes());
        Assert.assertEquals(0, byteBuf.readerIndex());
    }

    @Test
    public void testCompositeByteBufReadIndex() {
        CompositeByteBuf compositeByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(4);

        ByteBuf byteBuf = Unpooled.directBuffer(10);
        writeRandomStrToByteBuf(byteBuf, 10);
        compositeByteBuf.addComponent(true, byteBuf.readBytes(5));
        readByteBuf(byteBuf);

        Assert.assertEquals(0, compositeByteBuf.readerIndex());
        Assert.assertEquals(5, compositeByteBuf.readableBytes());
        Assert.assertEquals(5, compositeByteBuf.writerIndex());

        byteBuf = Unpooled.directBuffer(10);
        writeRandomStrToByteBuf(byteBuf, 10);
        compositeByteBuf.addComponent(true, byteBuf);

        Assert.assertEquals(0, compositeByteBuf.readerIndex());
        Assert.assertEquals(15, compositeByteBuf.readableBytes());
        Assert.assertEquals(15, compositeByteBuf.writerIndex());
    }

    private void writeRandomStrToByteBuf(ByteBuf byteBuf, int len) {
        for (int i = 0; i < len; i++) {
            byteBuf.writeByte('a' + (int)(26 * Math.random()));
        }
    }

    private void readByteBuf(ByteBuf byteBuf) {
        readByteBuf(byteBuf, byteBuf.readableBytes());
    }

    private void readByteBuf(ByteBuf byteBuf, int len) {
        for (int i = 0; i < len; i++) {
            byteBuf.readByte();
        }
    }

}
