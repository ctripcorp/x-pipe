package com.ctrip.xpipe.redis.console.alert.message.subscriber;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManager;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Apr 20, 2018
 */
public class RepeatAlertEntitySubscriberManualTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private RepeatAlertEntitySubscriber subscriber;

    @Autowired
    private AlertPolicyManager policyManager;

    private AlertEntity alert;

    @Before
    public void beforeRepeatAlertEntitySubscriberTest() throws DalException {
        alert = new AlertEntity(new HostPort("192.168.1.10", 6379), dcNames[0],
                "clusterId", "shardId", "test message", ALERT_TYPE.XREDIS_VERSION_NOT_VALID);
    }

    @Test
    public void scheduledTask() throws InterruptedException {
        scheduled.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                subscriber.processData(alert);
            }
        }, 100, 100, TimeUnit.MILLISECONDS);
        TimeUnit.MINUTES.sleep(1);
    }

    @Test
    public void testScheduledReport() throws InterruptedException {
        subscriber.processData(alert);
        subscriber.processData(alert);
        subscriber.processData(alert);
        subscriber.processData(alert);
        subscriber.scheduledReport();
        Thread.sleep(2000);
    }

    @Test
    public void doProcessAlert() {
        subscriber.processData(alert);
    }


    /**No emails should be sent out, as the alert has been expired*/
    @Test
    public void testNoEmptyEmailOut() throws InterruptedException {
        alert.setAlertType(ALERT_TYPE.CLIENT_INSTANCE_NOT_OK);
        policyManager.markCheckInterval(ALERT_TYPE.CLIENT_INSTANCE_NOT_OK, ()->10);
        subscriber.doProcessAlert(alert);
        Thread.sleep(subscriber.recoveryMilli(alert) + 10);

        subscriber.scheduledReport();
        Thread.sleep(2000);
    }
}