package com.ctrip.xpipe.redis.console.alert.decorator;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.decorator.GroupedAlertMessageDecorator;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Oct 20, 2017
 */
public class GroupedAlertMessageDecoratorTest extends AbstractConsoleIntegrationTest {

    @Autowired
    @Qualifier(GroupedAlertMessageDecorator.ID)
    private GroupedAlertMessageDecorator decorator;

    @Test
    public void generateTitle() throws Exception {
        Assert.assertEquals("[][XPipe 报警]", decorator.generateTitle());
    }

    @Test
    public void generateBody() throws Exception {
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
        alerts.put(ALERT_TYPE.CLIENT_INSTANCE_NOT_OK,
                Sets.newHashSet(
                        new AlertEntity(hostPort, dcNames[0], "cluster-test", "shard-test", "", ALERT_TYPE.CLIENT_INSTANCE_NOT_OK),
                        new AlertEntity(hostPort, dcNames[0], "cluster-test-1", "shard-test-1", "", ALERT_TYPE.CLIENT_INSTANCE_NOT_OK),
                        new AlertEntity(hostPort, dcNames[0], "cluster-test-2", "shard-test-2", "", ALERT_TYPE.CLIENT_INSTANCE_NOT_OK)
                        ));
        String body = decorator.generateBody(alerts);
        logger.info("{}", body);
    }

}