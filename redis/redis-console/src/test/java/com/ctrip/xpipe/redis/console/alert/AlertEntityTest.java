package com.ctrip.xpipe.redis.console.alert;

import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
public class AlertEntityTest {

    @Test
    public void getKey() throws Exception {
        String key = ALERT_TYPE.XREDIS_VERSION_NOT_VALID + ":";
        System.out.println(key);
    }

}