package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class ByteUtilTest extends AbstractTest {

    @Test
    public void testParseIntValidCases() {
        Assert.assertEquals(0, ByteUtil.parseInt("0".getBytes(), 0, 1, false));
        Assert.assertEquals(12345, ByteUtil.parseInt("12345".getBytes(), 0, 5, false));
        Assert.assertEquals(-12345, ByteUtil.parseInt("-12345".getBytes(), 0, 6, true));
        Assert.assertEquals(Integer.MAX_VALUE, ByteUtil.parseInt(String.valueOf(Integer.MAX_VALUE).getBytes(), 0, 10, false));
        Assert.assertEquals(Integer.MIN_VALUE, ByteUtil.parseInt(String.valueOf(Integer.MIN_VALUE).getBytes(), 0, 11, true));
        Assert.assertEquals(12, ByteUtil.parseInt("xx12yy".getBytes(), 2, 4, false));
        Assert.assertEquals(-12, ByteUtil.parseInt("xx-12yy".getBytes(), 2, 5, true));
        Assert.assertEquals(12, ByteUtil.parseInt("0012".getBytes(), 0, 4, false));
    }

    @Test
    public void testParseLongValidCases() {
        Assert.assertEquals(0L, ByteUtil.parseLong("0".getBytes(), 0, 1, false));
        Assert.assertEquals(123456789012345L, ByteUtil.parseLong("123456789012345".getBytes(), 0, 15, false));
        Assert.assertEquals(-123456789012345L, ByteUtil.parseLong("-123456789012345".getBytes(), 0, 16, true));
        Assert.assertEquals(Long.MAX_VALUE, ByteUtil.parseLong(String.valueOf(Long.MAX_VALUE).getBytes(), 0, 19, false));
        Assert.assertEquals(Long.MIN_VALUE, ByteUtil.parseLong(String.valueOf(Long.MIN_VALUE).getBytes(), 0, 20, true));
        Assert.assertEquals(9876543210L, ByteUtil.parseLong("aa9876543210bb".getBytes(), 2, 12, false));
    }

    @Test
    public void testParseIntInvalidFormatAndOverflow() {
        assertNumberFormatContains(() -> ByteUtil.parseInt("12a".getBytes(), 0, 3, false), "12a");
        assertNumberFormatContains(() -> ByteUtil.parseInt("-".getBytes(), 0, 1, true), "-");
        assertNumberFormatContains(() -> ByteUtil.parseInt("-1".getBytes(), 0, 2, false), "-1");
        assertNumberFormatContains(() -> ByteUtil.parseInt("2147483648".getBytes(), 0, 10, false), "2147483648");
        assertNumberFormatContains(() -> ByteUtil.parseInt("-2147483649".getBytes(), 0, 11, true), "-2147483649");
    }

    @Test
    public void testParseLongInvalidFormatAndOverflow() {
        assertNumberFormatContains(() -> ByteUtil.parseLong("99x".getBytes(), 0, 3, false), "99x");
        assertNumberFormatContains(() -> ByteUtil.parseLong("-".getBytes(), 0, 1, true), "-");
        assertNumberFormatContains(() -> ByteUtil.parseLong("-2".getBytes(), 0, 2, false), "-2");
        assertNumberFormatContains(() -> ByteUtil.parseLong("9223372036854775808".getBytes(), 0, 19, false), "9223372036854775808");
        assertNumberFormatContains(() -> ByteUtil.parseLong("-9223372036854775809".getBytes(), 0, 20, true), "-9223372036854775809");
    }

    @Test
    public void testParseIntInvalidRange() {
        assertIllegalArgument(() -> ByteUtil.parseInt(null, 0, 1, false));
        assertIllegalArgument(() -> ByteUtil.parseInt("123".getBytes(), -1, 1, false));
        assertIllegalArgument(() -> ByteUtil.parseInt("123".getBytes(), 0, 4, false));
        assertIllegalArgument(() -> ByteUtil.parseInt("123".getBytes(), 2, 2, false));
        assertIllegalArgument(() -> ByteUtil.parseInt("123".getBytes(), 3, 2, false));
    }

    @Test
    public void testParseLongInvalidRange() {
        assertIllegalArgument(() -> ByteUtil.parseLong(null, 0, 1, false));
        assertIllegalArgument(() -> ByteUtil.parseLong("123".getBytes(), -1, 1, false));
        assertIllegalArgument(() -> ByteUtil.parseLong("123".getBytes(), 0, 4, false));
        assertIllegalArgument(() -> ByteUtil.parseLong("123".getBytes(), 2, 2, false));
        assertIllegalArgument(() -> ByteUtil.parseLong("123".getBytes(), 3, 2, false));
    }

    private void assertIllegalArgument(ThrowingRunnable action) {
        try {
            action.run();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    private void assertNumberFormatContains(ThrowingRunnable action, String expectedFragment) {
        try {
            action.run();
            Assert.fail("expected NumberFormatException");
        } catch (NumberFormatException expected) {
            Assert.assertTrue(expected.getMessage().contains(expectedFragment));
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run();
    }
}
