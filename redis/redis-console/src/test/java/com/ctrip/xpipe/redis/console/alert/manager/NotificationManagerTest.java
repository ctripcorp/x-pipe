package com.ctrip.xpipe.redis.console.alert.manager;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.utils.DateTimeUtils;
import javafx.scene.control.Alert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 19, 2017
 */
public class NotificationManagerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    NotificationManager notificationManager;

    String cluster, shard, message;
    HostPort hostPort;

    @Before
    public void beforeNotificationManagerTest() {
        cluster = "cluster-test";
        shard = "shard-test";
        message = "test-message";
        hostPort = new HostPort("192.168.1.10", 6379);

    }

    @Test
    public void addAlert() throws Exception {
        notificationManager.addAlert(cluster, shard, hostPort, ALERT_TYPE.CLIENT_INCONSIS, message);
        // Sleep to see if email has been sent
        Thread.sleep(3000);
        // Only one should be sent
        notificationManager.addAlert(cluster, shard, hostPort, ALERT_TYPE.CLIENT_INCONSIS, message);
        Thread.sleep(3000);
        notificationManager.addAlert(cluster+2, shard, hostPort, ALERT_TYPE.CLIENT_INCONSIS, message);
        Thread.sleep(3000);
        notificationManager.addAlert(cluster+2, shard, hostPort, ALERT_TYPE.CLIENT_INCONSIS, message);
        Thread.sleep(3000);
    }

    @Test
    public void isSuspend() throws Exception {
        AlertEntity alert = new AlertEntity(hostPort, cluster, shard, message, ALERT_TYPE.CLIENT_INCONSIS);
        notificationManager.addAlert(cluster, shard, hostPort, ALERT_TYPE.CLIENT_INCONSIS, message);
        Assert.assertFalse(notificationManager.isSuspend(alert.getKey(), 1000));
        Thread.sleep(1000 * 60);
        Assert.assertTrue(notificationManager.isSuspend(alert.getKey(), 1));
    }

    @Test
    public void send() throws Exception {
        AlertEntity alert = new AlertEntity(hostPort, cluster, shard, message, ALERT_TYPE.CLIENT_INCONSIS);
        Assert.assertTrue(notificationManager.send(alert));
        notificationManager.addAlert(cluster, shard, hostPort, ALERT_TYPE.CLIENT_INCONSIS, message);
        Assert.assertFalse(notificationManager.send(alert));
    }

    @Test
    public void sendRecoveryMessage() throws Exception {
        AlertEntity alert = new AlertEntity(hostPort, cluster, shard, message, ALERT_TYPE.CLIENT_INCONSIS);
        Assert.assertTrue(notificationManager.sendRecoveryMessage(alert, DateTimeUtils.currentTimeAsString()));
    }

}