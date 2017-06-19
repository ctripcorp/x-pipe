package com.ctrip.xpipe.api.sso;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 12, 2017
 */
public class SsoConfigTest extends AbstractTest{

    @Test
    public void testSsoConfigTest(){

        Assert.assertTrue(SsoConfig.excludes("/api/"));
        Assert.assertTrue(SsoConfig.excludes("/api/abc"));
        Assert.assertFalse(SsoConfig.excludes( "/api"));
        Assert.assertTrue(SsoConfig.excludes("/health"));

    }
}
