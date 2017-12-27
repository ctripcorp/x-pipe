package com.ctrip.xpipe.redis.console.alert.manager;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Oct 19, 2017
 */
public class DecoratorManagerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    DecoratorManager decoratorManager;

    AlertEntity alert;

    @Before
    public void beforeAlertPolicyManagerTest() {
        alert = new AlertEntity(new HostPort("192.168.1.10", 6379), dcNames[0],
                "clusterId", "shardId", "test message", ALERT_TYPE.XREDIS_VERSION_NOT_VALID);
    }

    @Test
    public void generateTitleAndContent() throws Exception {
        Pair<String, String> pair = decoratorManager.generateTitleAndContent(alert, true);
        String title = pair.getKey();
        String content = pair.getValue();
        logger.info("title: {}", title);
        logger.info("content: {}",content);

    }

    @Test
    public void generateTitleTooLong() throws Exception {
        String message = "[PRO][XPipe 恢复]CLIENT_INCONSIS:train_tieyoubooking_redis:shard not equal:";
        while(message.length() <= 1024) {
            message += "{cluster: test-cluster, shard: shard-test, hostport: {'192.168.1.10:6379', '192.168.1.10:6379'\n}}";
        }
        alert.setMessage(message);

        Pair<String, String> pair = decoratorManager.generateTitleAndContent(alert, true);
        String title = pair.getKey();
        Assert.assertTrue(title.length() < 1024);
    }

}