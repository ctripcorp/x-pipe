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
        Assert.assertTrue(SsoConfig.matches("/api/"));
        Assert.assertTrue(SsoConfig.matches("/api/abc"));
        Assert.assertFalse(SsoConfig.matches( "/api"));
        Assert.assertTrue(SsoConfig.matches("/health"));

    }
}
