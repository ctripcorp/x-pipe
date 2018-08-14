package com.ctrip.xpipe.redis.console.alert.message.subscriber;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManager;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.util.concurrent.TimeUnit;

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

    @Autowired
    private ConfigService configService;

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
    public void testScheduledReport() throws InterruptedException, DalException {
        subscriber.processData(alert);
        subscriber.processData(alert);
        subscriber.processData(alert);
        subscriber.processData(alert);
        AlertEntity alert2 = new AlertEntity(new HostPort("192.168.2.10", 6390), dcNames[0],
                "clusterId2", "shardId2", "test message2", ALERT_TYPE.CLIENT_INSTANCE_NOT_OK);
        AlertEntity alert3 = new AlertEntity(new HostPort("192.168.3.10", 6390), dcNames[0],
                "clusterId3", "shardId3", "test message3", ALERT_TYPE.CLIENT_INSTANCE_NOT_OK);
        AlertEntity alert4 = new AlertEntity(new HostPort("192.168.4.10", 6390), dcNames[0],
                "clusterId4", "shardId4", "test message4", ALERT_TYPE.CLIENT_INSTANCE_NOT_OK);
        subscriber.processData(alert2);
        subscriber.processData(alert3);
        subscriber.processData(alert4);

        configService.stopAlertSystem(new ConfigModel(), 1);

        subscriber.scheduledReport();
        Thread.sleep(2000);
    }

    @Test
    public void doProcessAlert() {
        AlertEntity alertEntity = new AlertEntity(new HostPort(), dcNames[0], "cluster", "shard", "instance is not valid", ALERT_TYPE.CLIENT_INSTANCE_NOT_OK);
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