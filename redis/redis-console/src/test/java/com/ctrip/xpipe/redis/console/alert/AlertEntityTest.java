package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.endpoint.HostPort;
import org.junit.Assert;
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

    @Test
    public void testRemoveSpecialCharacters() {
        AlertEntity alertEntity = new AlertEntity(new HostPort("", 6379), "dc", "cluster", "shard", "message\nmessage1",
                ALERT_TYPE.CLIENT_INCONSIS);
        System.out.println(alertEntity.getMessage());
        alertEntity.removeSpecialCharacters();
        System.out.println(alertEntity.getMessage());
        Assert.assertFalse(alertEntity.getMessage().contains("\n"));
    }

}