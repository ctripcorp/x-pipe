package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 17, 2017
 */
public class MathUtilTest extends AbstractTest {

    @Test
    public void testSum() {

        Assert.assertEquals(Integer.MAX_VALUE, MathUtil.sum(Integer.MAX_VALUE, 0, 0));
        Assert.assertEquals(Integer.MAX_VALUE, MathUtil.sum(Integer.MAX_VALUE - 1, 1, 0));
        try {
            MathUtil.sum(Integer.MAX_VALUE, 1);
            Assert.fail();
        } catch (IllegalArgumentException e) {
        }

        Assert.assertEquals(Integer.MIN_VALUE, MathUtil.sum(Integer.MIN_VALUE, 0, 0));
        Assert.assertEquals(Integer.MIN_VALUE, MathUtil.sum(Integer.MIN_VALUE + 1, -1, 0));
        try {
            MathUtil.sum(Integer.MIN_VALUE, -1);
            Assert.fail();
        } catch (IllegalArgumentException e) {
        }


    }
}
