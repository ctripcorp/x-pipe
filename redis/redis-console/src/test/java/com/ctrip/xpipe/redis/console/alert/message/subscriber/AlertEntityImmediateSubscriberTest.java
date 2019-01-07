package com.ctrip.xpipe.redis.console.alert.message.subscriber;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Apr 20, 2018
 */

/**
 * Before Manual Test, Modify AlertEntityImmediateSubscriber's schedule periodic to TimeUnit.SECONDS
 * */
public class AlertEntityImmediateSubscriberTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private AlertEntityImmediateSubscriber subscriber;

    @Autowired
    private AlertPolicyManager policyManager;

    private AlertEntity alert;

    @Before
    public void beforeAlertEntityImmediateSubscriberTest() {
        alert = new AlertEntity(new HostPort("192.168.1.10", 6379), dcNames[0],
                "clusterId", "shardId", "test message", ALERT_TYPE.XREDIS_VERSION_NOT_VALID);
    }

    @Test
    public void initCleaner() {
    }

    @Test  // should see from log, send only once
    public void doProcessAlert() {

        subscriber.doProcessAlert(alert);
        sleep(50);
        subscriber.doProcessAlert(newAlert(alert));
        sleep(50);
        subscriber.doProcessAlert(newAlert(alert));
        sleep(50);
        subscriber.doProcessAlert(newAlert(alert));
        Set<AlertEntity> alertEntities = subscriber.getExistingAlerts();
        Assert.assertEquals(1, alertEntities.size());
        for(AlertEntity alertEntity : alertEntities) {
            logger.info("[AlertEntity] {}", alertEntity);
            Assert.assertTrue(System.currentTimeMillis() - alertEntity.getDate().getTime() < 50);
        }
    }

    private AlertEntity newAlert(AlertEntity alert) {
        return new AlertEntity(alert.getHostPort(), alert.getDc(), alert.getClusterId(), alert.getShardId(), alert.getMessage(), alert.getAlertType());
    }

    @Test  // should see from log, send twice
    public void doProcessAlert2() throws InterruptedException {
        policyManager.markCheckInterval(ALERT_TYPE.XREDIS_VERSION_NOT_VALID, ()->10);

        subscriber.doProcessAlert(alert);

        Thread.sleep(1500);
        logger.info("Sleep time up");
        subscriber.doProcessAlert(alert);
        subscriber.doProcessAlert(alert);
    }

    @Test  // should see from log, send only once
    public void doProcessAlert3() throws InterruptedException {
        policyManager.markCheckInterval(ALERT_TYPE.XREDIS_VERSION_NOT_VALID, ()->1000);

        subscriber.doProcessAlert(alert);

        Thread.sleep(1500);
        logger.info("Sleep time up");
        subscriber.doProcessAlert(alert);
        subscriber.doProcessAlert(alert);
    }
}