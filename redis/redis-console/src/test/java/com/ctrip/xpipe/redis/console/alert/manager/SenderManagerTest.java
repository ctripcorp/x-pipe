package com.ctrip.xpipe.redis.console.alert.manager;

import com.ctrip.xpipe.api.email.EmailType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.console.alert.sender.EmailSender;
import com.ctrip.xpipe.redis.console.alert.sender.Sender;
import io.netty.util.internal.ConcurrentSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
public class SenderManagerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    SenderManager senderManager;

    @Before
    public void beforeSenderManagerTest() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void querySender() throws Exception {
        Sender sender = senderManager.querySender(EmailSender.ID);
        Assert.assertTrue(sender instanceof EmailSender);
        AlertChannel channel = AlertChannel.MAIL;
        String id = channel.getId();
        sender = senderManager.querySender(id);
        Assert.assertTrue(sender instanceof EmailSender);
    }

    @Test
    public void sendAlert() throws Exception {
        String title = "Test";
        String content = "Test Content";
        List<String> recepients = Arrays.asList("test1@gmail.com, test2@gmail.com");
        senderManager.sendAlert(AlertChannel.MAIL,
                new AlertMessageEntity(title, EmailType.CONSOLE_ALERT, content, recepients));
    }

    @Test
    public void sendAlerts() throws Exception {
        HostPort hostPort = new HostPort("192.168.1.10", 6379);
        Map<ALERT_TYPE, Set<AlertEntity>> alerts = new HashMap<>();
        alerts.put(ALERT_TYPE.CLIENT_INCONSIS,
                Collections.singleton(
                        new AlertEntity(hostPort, dcNames[0], "cluster-test", "shard-test", "", ALERT_TYPE.CLIENT_INCONSIS
                        )));
        alerts.put(ALERT_TYPE.XREDIS_VERSION_NOT_VALID,
                Collections.singleton(
                        new AlertEntity(hostPort, dcNames[0], "cluster-test", "shard-test", "", ALERT_TYPE.XREDIS_VERSION_NOT_VALID
                        )));
        alerts.put(ALERT_TYPE.QUORUM_DOWN_FAIL,
                Collections.singleton(
                        new AlertEntity(hostPort, dcNames[0], "cluster-test", "shard-test", "", ALERT_TYPE.QUORUM_DOWN_FAIL
                        )));
        alerts.put(ALERT_TYPE.SENTINEL_RESET,
                Collections.singleton(
                        new AlertEntity(hostPort, dcNames[0], "cluster-test", "shard-test", "", ALERT_TYPE.SENTINEL_RESET
                        )));
        alerts.put(ALERT_TYPE.REDIS_CONF_REWRITE_FAILURE,
                Collections.singleton(
                        new AlertEntity(hostPort, dcNames[0], "cluster-test", "shard-test", "", ALERT_TYPE.REDIS_CONF_REWRITE_FAILURE
                        )));
        alerts.put(ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR,
                Collections.singleton(
                        new AlertEntity(hostPort, dcNames[0], "cluster-test", "shard-test", "", ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR
                        )));
        alerts.put(ALERT_TYPE.MIGRATION_MANY_UNFINISHED,
                Collections.singleton(
                        new AlertEntity(hostPort, dcNames[0], "cluster-test", "shard-test", "", ALERT_TYPE.MIGRATION_MANY_UNFINISHED
                        )));
        logger.info("{}", alerts);
        senderManager.sendAlerts(alerts);
    }

    @Test
    public void testSenderManager() {
        HostPort hostPort = new HostPort("192.168.1.10", 6379);
        Map<ALERT_TYPE, Set<AlertEntity>> alerts = new ConcurrentHashMap<>();
        AlertEntity alert = new AlertEntity(hostPort, dcNames[0], "cluster-test", "shard-test", "", ALERT_TYPE.XREDIS_VERSION_NOT_VALID);
        Set<AlertEntity> set = new ConcurrentSet<>();
        set.add(alert);
        alerts.put(ALERT_TYPE.XREDIS_VERSION_NOT_VALID, set);

        new Thread(new Runnable() {
            @Override
            public void run() {
                alerts.get(ALERT_TYPE.XREDIS_VERSION_NOT_VALID).remove(alert);
            }
        }).start();
        List<Map<ALERT_TYPE, Set<AlertEntity>>> result = senderManager.getGroupedAlerts(alerts);
        logger.info("result: {}", result.get(0));
        if(!result.isEmpty()) {
            Set<AlertEntity> alertEntities = result.get(0).getOrDefault(alert.getAlertType(), null);
            if(alertEntities != null) {
                Assert.assertFalse(alertEntities.isEmpty());
            }
        }
    }

}