package com.ctrip.xpipe.redis.console.alert.message.subscriber;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManager;
import com.ctrip.xpipe.redis.console.alert.manager.DecoratorManager;
import com.ctrip.xpipe.redis.console.alert.manager.SenderManager;
import com.ctrip.xpipe.redis.console.alert.policy.timing.RecoveryTimeSlotControl;
import com.ctrip.xpipe.redis.console.alert.policy.timing.TimeSlotControl;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        subscriber.doProcessAlert(alert);
        subscriber.doProcessAlert(alert);
        subscriber.doProcessAlert(alert);
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