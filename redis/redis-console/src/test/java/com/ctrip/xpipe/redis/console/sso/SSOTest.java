package com.ctrip.xpipe.redis.console.sso;

import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.redis.console.AbstractIntegrationTest;
import com.ctrip.xpipe.redis.console.constant.XpipeConsoleConstant;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author lepdou 2016-11-09
 */
public class SSOTest extends AbstractIntegrationTest {

    @Autowired
    private UserInfoHolder userInfoHolder;

    @Test
    public void test() {
        Assert.assertEquals(XpipeConsoleConstant.DEFAULT_XPIPE_USER,
                            userInfoHolder.getUser().getUserId());
    }

}
