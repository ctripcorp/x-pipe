package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.payload.DirectByteBufInOutPayload;
import com.ctrip.xpipe.payload.InOutPayloadFactory;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

public class ArrayParserResetTest extends AbstractRedisProtocolTest {

    @Test
    public void testResetReleasesInFlightDirectPayload() {
        ArrayParser arrayParser = new ArrayParser().setInOutPayloadFactory(new InOutPayloadFactory() {
            @Override
            public com.ctrip.xpipe.api.payload.InOutPayload create() {
                return new DirectByteBufInOutPayload();
            }
        });

        ByteBuf partial = Unpooled.wrappedBuffer("*2\r\n$3\r\nSET\r\n$".getBytes());
        RedisClientProtocol<Object[]> result = arrayParser.read(partial);
        Assert.assertNull(result);

        arrayParser.reset();

        ByteBuf complete = Unpooled.wrappedBuffer(
                "*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n".getBytes());
        result = arrayParser.read(complete);
        Assert.assertNotNull(result);
        Object[] payload = result.getPayload();
        Assert.assertEquals(2, payload.length);
        Assert.assertTrue(payload[0] instanceof DirectByteBufInOutPayload);
        Assert.assertEquals("SET", payload[0].toString());
    }

    @Test
    public void testDefaultFactoryUsesHeapPayload() {
        ArrayParser arrayParser = new ArrayParser();
        ByteBuf byteBuf = Unpooled.wrappedBuffer("*1\r\n$3\r\nSET\r\n".getBytes());
        RedisClientProtocol<Object[]> result = arrayParser.read(byteBuf);
        Assert.assertNotNull(result);
        Object[] payload = result.getPayload();
        Assert.assertTrue(payload[0] instanceof ByteArrayOutputStreamPayload);
    }
}
