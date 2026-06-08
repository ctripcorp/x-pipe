package com.ctrip.xpipe.payload;

import com.ctrip.xpipe.AbstractTest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class DirectByteBufInOutPayloadTest extends AbstractTest {

    @Test
    public void testGetBytesDoesNotMoveReaderIndex() throws IOException {
        DirectByteBufInOutPayload payload = new DirectByteBufInOutPayload();
        payload.startInput();
        payload.in(Unpooled.wrappedBuffer("hello".getBytes()));

        int readerIndexBefore = payload.toString().length();
        byte[] first = payload.getBytes();
        byte[] second = payload.getBytes();

        Assert.assertTrue(Arrays.equals(first, second));
        Assert.assertEquals("hello", new String(first));
        Assert.assertEquals("hello", payload.toString());
        Assert.assertEquals(5, readerIndexBefore);
    }

    @Test
    public void testResetReleasesCumulation() throws IOException {
        DirectByteBufInOutPayload payload = new DirectByteBufInOutPayload();
        payload.startInput();
        ByteBuf buf = Unpooled.wrappedBuffer("value".getBytes());
        payload.in(buf);

        payload.reset();
        payload.startInput();
        payload.in(Unpooled.wrappedBuffer("new".getBytes()));
        Assert.assertEquals("new", payload.toString());
    }

    @Test
    public void testEqualsIgnoreCaseAsciiExpectedUppercase() throws IOException {
        DirectByteBufInOutPayload payload = new DirectByteBufInOutPayload();
        payload.startInput();
        payload.in(Unpooled.wrappedBuffer("MULTI".getBytes()));

        Assert.assertTrue(payload.equalsIgnoreCaseAsciiExpectedUppercase(new byte[]{'M', 'U', 'L', 'T', 'I'}));
        Assert.assertFalse(payload.equalsIgnoreCaseAsciiExpectedUppercase(new byte[]{'E', 'X', 'E', 'C'}));
    }
}
