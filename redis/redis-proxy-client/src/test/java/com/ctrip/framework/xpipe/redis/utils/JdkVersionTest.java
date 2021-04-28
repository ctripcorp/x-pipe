package com.ctrip.framework.xpipe.redis.utils;

import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.framework.xpipe.redis.utils.JdkVersion.JAVA_VERSION;

/**
 * @Author limingdong
 * @create 2021/4/28
 */
public class JdkVersionTest {

    private static final String JDK7 = "1.7.0_80";

    private static final String JDK11 = "11.0.4";

    @Test
    public void testVersion() {
        System.setProperty(JAVA_VERSION, JDK7);
        int actualVersion = JdkVersion.getVersion();
        Assert.assertEquals(7, actualVersion);

        System.setProperty(JAVA_VERSION, JDK11);
        actualVersion = JdkVersion.getVersion();
        Assert.assertEquals(11, actualVersion);
    }

}