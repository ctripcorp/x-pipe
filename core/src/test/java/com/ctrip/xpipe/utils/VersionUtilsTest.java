package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 23, 2018
 */
public class VersionUtilsTest extends AbstractTest {


    @Test
    public void testEqual() {

        Assert.assertTrue(VersionUtils.equal("1.0.1", "1.0.1"));
        Assert.assertTrue(VersionUtils.equal("1.0", "1.0.0"));
        Assert.assertTrue(VersionUtils.equal("10000.0", "10000.0.0"));

    }

    @Test
    public void testGt() {

        Assert.assertTrue(VersionUtils.gt("1.0.1", "1.0.0"));
        Assert.assertFalse(VersionUtils.gt("1.0.1", "1.0.1"));
        Assert.assertTrue(VersionUtils.gt("11", "1.0.1"));
        Assert.assertTrue(VersionUtils.gt("11.0.0", "1.0"));

    }

    @Test
    public void testGe() {
        Assert.assertTrue(VersionUtils.ge("1.0.1", "1.0.1"));
    }

}
