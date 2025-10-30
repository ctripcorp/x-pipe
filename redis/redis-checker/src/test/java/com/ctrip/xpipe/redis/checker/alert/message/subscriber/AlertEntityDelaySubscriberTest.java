package com.ctrip.xpipe.redis.checker.alert.message.subscriber;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.manager.AlertPolicyManager;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Set;

/**
 * @author qifanwang
 * <p>
 * July 1, 2024
 */

public class AlertEntityDelaySubscriberTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private AlertEntityDelaySubscriber subscriber;

    private AlertEntity alert;

    @Autowired
    private AlertRecoverySubscriber recoverySubscriber;

    @Autowired
    private AlertPolicyManager policyManager;

    @Before
    public void beforeAlertRecoverySubscriberTest() {
        alert = new AlertEntity(new HostPort("192.168.1.10", 6379), dcNames[0],
                "clusterId", "shardId", "test message", ALERT_TYPE.CLIENT_INCONSIS);
        subscriber.setReportInterval(0);
    }

    private AlertEntity newAlert(AlertEntity alert) {
        return new AlertEntity(alert.getHostPort(), alert.getDc(), alert.getClusterId(),
                alert.getShardId(), alert.getMessage(), alert.getAlertType());
    }

    private AlertEntity newDelayAlert(AlertEntity alert) {
        AlertEntity newAlert = new AlertEntity(alert.getHostPort(), alert.getDc(), alert.getClusterId(),
                alert.getShardId(), alert.getMessage(), alert.getAlertType());
        newAlert.setDate(DateTimeUtils.getMinutesLaterThan(alert.getDate(),
                (int) alert.getAlertType().delayedSendingTime() / (1000 * 60)));
        return newAlert;
    }

    @Test  // should see from log, send only once
    @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
    public void doProcessAlert() throws InterruptedException {

        subscriber.doProcessAlert(alert);
        sleep(50);
        subscriber.doProcessAlert(newAlert(alert));
        Set<AlertEntity> alertEntities = subscriber.getExistingAlerts();
        Set<AlertEntity> sending = subscriber.getSendingAlerts();

        Assert.assertEquals(1, alertEntities.size());
        Assert.assertEquals(0, sending.size());

        sleep(50);
        subscriber.doProcessAlert(newDelayAlert(alert));
        alertEntities = subscriber.getExistingAlerts();
        sending = subscriber.getSendingAlerts();

        Assert.assertEquals(1, alertEntities.size());
        Assert.assertEquals(1, sending.size());
    }

    @Test
    @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
    public void testRecovery() throws InterruptedException {

        policyManager.markCheckInterval(ALERT_TYPE.CLIENT_INCONSIS, ()->10);

        subscriber.doProcessAlert(alert);
        sleep(50);
        subscriber.doProcessAlert(newAlert(alert));
        subscriber.doProcessAlert(newDelayAlert(alert));

        recoverySubscriber.doProcessAlert(alert);
        recoverySubscriber.reportRecovered();

        Set<AlertEntity> alertEntities = recoverySubscriber.getExistingAlerts();

        Assert.assertEquals(1, alertEntities.size());

        Thread.sleep(subscriber.recoveryMilli(alert) + 10);
        recoverySubscriber.reportRecovered();
        sleep(1000);
        alertEntities = recoverySubscriber.getExistingAlerts();
        Assert.assertEquals(0, alertEntities.size());

        recoverySubscriber.doProcessAlert(alert);

        alertEntities = recoverySubscriber.getExistingAlerts();

        Assert.assertEquals(0, alertEntities.size());

    }



}
