package com.ctrip.xpipe.redis.console.sso;

import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.redis.console.AppTest;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author lepdou 2016-11-09
 */
public class SSOTest extends AppTest {

    @Autowired
    private UserInfoHolder userInfoHolder;

    @Test
    public void test() {
        Assert.assertEquals(XPipeConsoleConstant.DEFAULT_XPIPE_USER,
                            userInfoHolder.getUser().getUserId());
    }

}
