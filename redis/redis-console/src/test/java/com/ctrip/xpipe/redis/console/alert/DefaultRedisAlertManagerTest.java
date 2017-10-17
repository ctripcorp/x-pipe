package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 17, 2017
 */
public class DefaultRedisAlertManagerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    DefaultRedisAlertManager manager;

    @Test
    public void findOrCreateRedisAlert() throws Exception {
        manager.findOrCreateRedisAlert(ALERT_TYPE.XREDIS_VERSION_NOT_VALID, "cluster", "shard",
                new HostPort("192.168.1.10", 6379), "fake message");

    }

    @Test
    public void findOrCreateRedisAlert2() throws Exception {
        manager.cleanup();
        manager.findOrCreateRedisAlert(ALERT_TYPE.XREDIS_VERSION_NOT_VALID, "cluster", "shard",
                new HostPort("192.168.1.10", 6379), "fake message");
        manager.findOrCreateRedisAlert(ALERT_TYPE.XREDIS_VERSION_NOT_VALID, "cluster", "shard",
                new HostPort("192.168.1.10", 6379), "fake message");
        manager.findOrCreateRedisAlert(ALERT_TYPE.XREDIS_VERSION_NOT_VALID, "cluster", "shard",
                new HostPort("192.168.1.10", 6379), "fake message1");

        manager.findOrCreateRedisAlert(ALERT_TYPE.CLIENT_INCONSIS, "cluster", null,
                null, "fake message");
        manager.findOrCreateRedisAlert(ALERT_TYPE.CLIENT_INCONSIS, "cluster", null,
                null, "fake message1");
        manager.findOrCreateRedisAlert(ALERT_TYPE.CLIENT_INCONSIS, "cluster", null,
                null, "fake message2");
    }

}