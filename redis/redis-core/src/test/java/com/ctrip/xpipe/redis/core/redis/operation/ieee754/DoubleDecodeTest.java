/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 Glenn Lane
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.ctrip.xpipe.redis.core.redis.operation.ieee754;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class DoubleDecodeTest {
    private void testDecode(double expectedDouble) {
        System.out.printf("expectedDouble: %.2f\r\n", expectedDouble);
        byte[] expectedDoubleBuf = new byte[8];
        ByteBuffer.wrap(expectedDoubleBuf).asDoubleBuffer().put(0, expectedDouble);
        IEEE754 actualIeee = IEEE754.decode(
                IEEE754Format.DOUBLE, BitUtils.wrapSource(expectedDoubleBuf));
        if (Double.isNaN(expectedDouble)) {
            Assert.assertSame("decoding NaN", IEEE754.NaN, actualIeee);
            /*
             * Our library doesn't retain NaN values, so we'll narrow the value
             * and buffer to the NaN constant (only the first mantissa bit is
             * set)
             */
            expectedDouble = Double.NaN;
            ByteBuffer.wrap(expectedDoubleBuf).asDoubleBuffer().put(
                    0, expectedDouble);
        } else if (expectedDouble == Double.POSITIVE_INFINITY) {
            Assert.assertSame("decoding positive infinity", IEEE754.POSITIVE_INFINITY, actualIeee);
        } else if (expectedDouble == Double.NEGATIVE_INFINITY) {
            Assert.assertSame("decoding negative infinity", IEEE754.NEGATIVE_INFINITY, actualIeee);
        } else if (Double.doubleToLongBits(expectedDouble)
                == Double.doubleToLongBits(-0D)) {
            Assert.assertSame("decoding negative zero", IEEE754.NEGATIVE_ZERO, actualIeee);
        } else if (expectedDouble == 0D) {
            Assert.assertSame("decoding positive zero", IEEE754.POSITIVE_ZERO, actualIeee);
        } else {
            Assert.assertTrue("decoding number", actualIeee instanceof IEEE754.IEEE754Number);
            IEEE754.IEEE754Number in = (IEEE754.IEEE754Number) actualIeee;
            double actualDouble = in.getSignificand().doubleValue()
                    * Math.pow(2D, in.getExponent().doubleValue());
            Assert.assertEquals("correct exponent & significand", expectedDouble, actualDouble, 0D);
        }

        byte[] actualDoubleBuf = new byte[8];
        actualIeee.toBits(IEEE754Format.DOUBLE, BitUtils.wrapSink(actualDoubleBuf));
        Assert.assertArrayEquals("round-trip encoding", expectedDoubleBuf, actualDoubleBuf);

        byte[] expectedFloatBuf = new byte[4];
        ByteBuffer.wrap(expectedFloatBuf).asFloatBuffer().put(0, (float) expectedDouble);
        byte[] actualFloatBuf = new byte[4];
        actualIeee.toBits(IEEE754Format.SINGLE, BitUtils.wrapSink(actualFloatBuf));
        Assert.assertArrayEquals("downcast encoding", expectedFloatBuf, actualFloatBuf);
    }

    @Test
    public void decodeConstants() {
        testDecode(Double.POSITIVE_INFINITY);
        testDecode(Double.NEGATIVE_INFINITY);
        testDecode(Double.NaN);
        testDecode(0D);
        testDecode(-0D);
        testDecode(Double.MAX_VALUE);
        testDecode(Double.MIN_VALUE);
        testDecode(Double.MIN_NORMAL);
    }

    @Test
    public void decodeRandom() {
        testDecode(56.1594979763031);
    }

}
