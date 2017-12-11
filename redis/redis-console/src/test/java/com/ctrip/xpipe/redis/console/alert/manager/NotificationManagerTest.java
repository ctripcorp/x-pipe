package com.ctrip.xpipe.redis.console.alert.manager;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;

import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Oct 19, 2017
 */
public class NotificationManagerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    @InjectMocks
    NotificationManager notificationManager;

    @Mock
    CrossDcClusterServer mockServer;

    String cluster, shard, message;
    HostPort hostPort;

    @Before
    public void beforeNotificationManagerTest() {
        MockitoAnnotations.initMocks(this);
        cluster = "cluster-test";
        shard = "shard-test";
        message = "test-message";
        hostPort = new HostPort("192.168.1.10", 6379);
        when(mockServer.amILeader()).thenReturn(Boolean.TRUE);
    }

    @Test
    @DirtiesContext
    public void isSuspend() throws Exception {
        AlertEntity alert = new AlertEntity(hostPort, dcNames[0], cluster, shard, message, ALERT_TYPE.CLIENT_INCONSIS);
        notificationManager.addAlert(cluster, dcNames[0], shard, hostPort, ALERT_TYPE.CLIENT_INCONSIS, message);
        Thread.sleep(1000);
        Assert.assertTrue(notificationManager.isSuspend(alert.getKey(), 1000));
    }

    @Test
    @DirtiesContext
    public void send() throws Exception {
        AlertEntity alert = new AlertEntity(hostPort, dcNames[0], cluster, shard, message, ALERT_TYPE.CLIENT_INCONSIS);
        Assert.assertTrue(notificationManager.send(alert));
        notificationManager.addAlert(dcNames[0], cluster, shard, hostPort, ALERT_TYPE.CLIENT_INCONSIS, message);
        Assert.assertFalse(notificationManager.send(alert));
    }

    @Test
    @DirtiesContext
    public void sendRecoveryMessage() throws Exception {
        AlertEntity alert = new AlertEntity(hostPort, dcNames[0], cluster, shard, message, ALERT_TYPE.CLIENT_INCONSIS);
        Assert.assertTrue(notificationManager.sendRecoveryMessage(alert));

        alert = new AlertEntity(hostPort, dcNames[0], cluster, shard, message, ALERT_TYPE.MARK_INSTANCE_DOWN);
        notificationManager.sendRecoveryMessage(alert);
    }
}