package com.ctrip.xpipe.redis.console.alert.manager;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.policy.AlertPolicy;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.List;


/**
 * @author chen.zhu
 * <p>
 * Oct 19, 2017
 */
public class AlertPolicyManagerTest extends AbstractConsoleIntegrationTest {

    AlertEntity alert;

    @Autowired
    private AlertPolicyManager policyManager;

    @Autowired
    private ConfigService configService;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Before
    public void beforeAlertPolicyManagerTest() {
        alert = new AlertEntity(new HostPort("192.168.1.10", 6379), dcNames[0],
                "clusterId", "shardId", "test message", ALERT_TYPE.XREDIS_VERSION_NOT_VALID);
    }

    @Test
    public void queryChannels() throws Exception {

        List<AlertChannel> channels = policyManager.queryChannels(alert);
        List<AlertChannel> expected = Arrays.asList(AlertChannel.MAIL);
        Assert.assertEquals(expected, channels);
    }

    @Test
    public void queryRecoverMinute() throws Exception {
        int minute = policyManager.queryRecoverMinute(alert);
        int expect = alert.getAlertType().getCheckInterval();
        Assert.assertEquals(expect, minute);
    }

    @Test
    public void querySuspendMinute() throws Exception {
        int minute = policyManager.querySuspendMinute(alert);
        int expect = 30;
        Assert.assertEquals(expect, minute);
    }

    @Test
    public void queryRecepients() throws Exception {
        AlertPolicy policy = policyManager.alertPolicyMap.get(SendToDBAAlertPolicy.ID);
        List<String> expected = policy.queryRecipients(new AlertEntity(null, null, null, null, null, ALERT_TYPE.MARK_INSTANCE_UP));
        logger.info("[testQueryRecepients] emails: {}", expected);
        Assert.assertEquals(expected, policyManager.queryRecepients(alert));
    }

    @Test
    public void queryCCers() throws Exception {
        AlertPolicy policy = policyManager.alertPolicyMap.get(SendToDBAAlertPolicy.ID);
        List<String> expected = policy.queryCCers();
        Assert.assertEquals(expected, policyManager.queryCCers(alert));
    }

    @Test
    public void testQueryRecepients() throws Exception {
        AlertEntity entity = new AlertEntity(null, null, null, null,
                ALERT_TYPE.ALERT_SYSTEM_OFF.simpleDesc(), ALERT_TYPE.ALERT_SYSTEM_OFF);
        ALERT_TYPE type = entity.getAlertType();
        logger.info("type.getAlertMethod() & EMAIL_DBA: {}", type.getAlertMethod() & EMAIL_DBA);
        logger.info("type.getAlertMethod() & EMAIL_XPIPE_ADMIN: {}", type.getAlertMethod() & EMAIL_XPIPE_ADMIN);
        logger.info("type.getAlertMethod() & EMAIL_CLUSTER_ADMIN: {}", type.getAlertMethod() & EMAIL_CLUSTER_ADMIN);

        List<String> recievers = policyManager.queryRecepients(entity);
        logger.info("recievers: {}", recievers);
        StringBuffer sb = new StringBuffer();
        for(String emailAdr : recievers) {
            sb.append(emailAdr).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);

        Assert.assertEquals(sb.toString(), consoleConfig.getDBAEmails()+","+consoleConfig.getXPipeAdminEmails());
    }

    @Test
    public void testEmptyQueryRecipients() throws Exception {
        configService.stopAlertSystem(new ConfigModel(), 1);
        alert.setAlertType(ALERT_TYPE.MARK_INSTANCE_UP);
        List<String> result = policyManager.queryRecepients(alert);
        logger.info("[recipients] {}", result);
        Assert.assertFalse(result.isEmpty());
    }

}