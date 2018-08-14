package com.ctrip.xpipe.redis.console.alert.message.subscriber;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManager;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Apr 20, 2018
 */
public class AlertRecoverySubscriberTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private AlertRecoverySubscriber subscriber;

    @Autowired
    private AlertPolicyManager policyManager;

    private AlertEntity alert;

    @Before
    public void beforeAlertRecoverySubscriberTest() {
        alert = new AlertEntity(new HostPort("192.168.1.10", 6379), dcNames[0],
                "clusterId", "shardId", "test message", ALERT_TYPE.QUORUM_DOWN_FAIL);
    }

    @Test // see nothing
    public void reportRecovered() {
        subscriber.reportRecovered();
    }

    @Test // see nothing
    public void reportRecovered2() {
        subscriber.doProcessAlert(alert);
        subscriber.reportRecovered();
    }

    @Test // see 1 report
    public void reportRecovered3() throws InterruptedException {
        alert.setAlertType(ALERT_TYPE.CLIENT_INSTANCE_NOT_OK);
        policyManager.markCheckInterval(ALERT_TYPE.CLIENT_INSTANCE_NOT_OK, ()->10);
        subscriber.doProcessAlert(alert);
        Thread.sleep(subscriber.recoveryMilli(alert) + 10);
        subscriber.reportRecovered();
        Thread.sleep(1000);
    }
}