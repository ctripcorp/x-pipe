package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.ByteBuffer;

@RunWith(MockitoJUnitRunner.class)
public class VarIntTest {

    @Test
    public void testVarIntSize() {
        Assert.assertEquals(1, VarInt.varIntSize(0));
        Assert.assertEquals(1, VarInt.varIntSize(127));
        Assert.assertEquals(2, VarInt.varIntSize(128));
        Assert.assertEquals(2, VarInt.varIntSize(16383));
        Assert.assertEquals(3, VarInt.varIntSize(16384));
        Assert.assertEquals(5, VarInt.varIntSize(Integer.MAX_VALUE));
    }

    @Test
    public void testEncode() {
        int[] values = {0, 127, 128, 16383, 16384, Integer.MAX_VALUE};
        for (int value : values) {
            byte[] bytes = VarInt.encode(value);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            Assert.assertEquals(value, VarInt.getVarInt(buffer));
        }
    }

    @Test
    public void testDecodeArray() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(20);
        int[] values = {0, 127, 128, 16383, 16384, 34};
        for (int value : values) {
            byte[] bytes = VarInt.encode(value);
            buffer.put(bytes);
        }
        buffer.flip();
        int sum = VarInt.decodeArray(buffer, values.length - 1);
        int expectedSum = 0;
        for (int value : values) {
            expectedSum += value;
        }
        Assert.assertEquals(expectedSum, sum);
        System.out.println(sum);
    }
}
