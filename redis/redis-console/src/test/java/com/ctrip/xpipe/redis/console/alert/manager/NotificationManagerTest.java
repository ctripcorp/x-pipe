package com.ctrip.xpipe.redis.console.alert.manager;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.message.subscriber.AlertEntityImmediateSubscriber;
import com.ctrip.xpipe.redis.console.alert.message.subscriber.AlertRecoverySubscriber;
import com.ctrip.xpipe.redis.console.alert.message.subscriber.RepeatAlertEntitySubscriber;
import com.ctrip.xpipe.redis.console.job.event.Subscriber;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Set;

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
    public void testSubscribers() {
        List<Subscriber<AlertEntity>> subscribers = notificationManager.subscribers();
        Set<Class> subscriberList = Sets.newHashSet(AlertEntityImmediateSubscriber.class,
                AlertRecoverySubscriber.class, RepeatAlertEntitySubscriber.class);

        Assert.assertEquals(subscriberList.size(), subscribers.size());
        subscribers.forEach(subscriber -> Assert.assertTrue(subscriberList.contains(subscriber.getClass())));
    }

}