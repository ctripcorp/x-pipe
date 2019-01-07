package com.ctrip.xpipe.redis.console.alert.manager;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.message.AlertEntityHolderManager;
import com.ctrip.xpipe.redis.console.alert.message.holder.DefaultAlertEntityHolderManager;
import com.ctrip.xpipe.redis.console.alert.policy.receiver.EmailReceiverModel;
import com.ctrip.xpipe.redis.console.alert.policy.timing.NaiveRecoveryTimeAlgorithm;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * @author chen.zhu
 * <p>
 * Oct 19, 2017
 */
public class AlertPolicyManagerTest extends AbstractConsoleIntegrationTest {

    private AlertEntity alert;

    @Autowired
    private AlertPolicyManager policyManager;

    @Autowired
    private ConfigService configService;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Before
    public void beforeAlertPolicyManagerTest() throws DalException {
        alert = new AlertEntity(new HostPort("192.168.1.10", 6379), dcNames[0],
                "clusterId", "shardId", "test message", ALERT_TYPE.XREDIS_VERSION_NOT_VALID);
        configService.startAlertSystem(new ConfigModel());
    }

    @Test
    public void queryChannels() throws Exception {

        List<AlertChannel> channels = policyManager.queryChannels(alert);
        List<AlertChannel> expected = Arrays.asList(AlertChannel.MAIL);
        Assert.assertEquals(expected, channels);
    }

    @Test
    public void queryRecoverMinute() throws Exception {
        alert = new AlertEntity(new HostPort("192.168.1.10", 6379), dcNames[0],
                "clusterId", "shardId", "test message", ALERT_TYPE.QUORUM_DOWN_FAIL);
        long milli = policyManager.queryRecoverMilli(alert);
        long expect = TimeUnit.MINUTES.toMillis(consoleConfig.getAlertSystemRecoverMinute());
        Assert.assertEquals(expect, milli);

        alert = new AlertEntity(new HostPort("192.168.1.10", 6379), dcNames[0],
                "clusterId", "shardId", "test message", ALERT_TYPE.CLIENT_INCONSIS);
        milli = policyManager.queryRecoverMilli(alert);
        expect = consoleConfig.getRedisConfCheckIntervalMilli();
        Assert.assertEquals(expect, milli);
    }

    @Test
    public void querySuspendMinute() throws Exception {
        long minute = policyManager.querySuspendMilli(alert);
        long expect = TimeUnit.MINUTES.toMillis(consoleConfig.getAlertSystemSuspendMinute());
        Assert.assertEquals(expect, minute);
    }

    @Test
    public void queryRecepients() throws Exception {
        alert = new AlertEntity(new HostPort("192.168.1.10", 6379), dcNames[0],
                "clusterId", "shardId", "test message", ALERT_TYPE.QUORUM_DOWN_FAIL);
        EmailReceiverModel receivers = policyManager.queryEmailReceivers(alert);
        EmailReceiverModel expect = new EmailReceiverModel(Lists.newArrayList(consoleConfig.getXPipeAdminEmails()), null);
        Assert.assertEquals(expect, receivers);

        configService.stopAlertSystem(new ConfigModel(), 1);
        alert = new AlertEntity(new HostPort("192.168.1.10", 6379), dcNames[0],
                "clusterId", "shardId", "test message", ALERT_TYPE.MARK_INSTANCE_UP);
        EmailReceiverModel receivers2 = policyManager.queryEmailReceivers(alert);
        EmailReceiverModel expect2 = new EmailReceiverModel(Lists.newArrayList(consoleConfig.getXPipeAdminEmails()), null);
        Assert.assertEquals(expect2, receivers2);

        configService.stopAlertSystem(new ConfigModel(), 1);
        alert = new AlertEntity(new HostPort("192.168.1.10", 6379), dcNames[0],
                "clusterId", "shardId", "test message", ALERT_TYPE.CLIENT_INCONSIS);
        EmailReceiverModel receivers3 = policyManager.queryEmailReceivers(alert);
        EmailReceiverModel expect3 = new EmailReceiverModel(Lists.newArrayList(consoleConfig.getXPipeAdminEmails()), null);
        Assert.assertEquals(expect3, receivers3);

        configService.stopAlertSystem(new ConfigModel(), 1);
        alert = new AlertEntity(new HostPort("192.168.1.10", 6379), dcNames[0],
                "clusterId", "shardId", "test message", ALERT_TYPE.ALERT_SYSTEM_OFF);
        EmailReceiverModel receivers4 = policyManager.queryEmailReceivers(alert);
        List<String> expectedReceivers = Lists.newArrayList(consoleConfig.getDBAEmails(), consoleConfig.getXPipeAdminEmails());
        EmailReceiverModel expect4 = new EmailReceiverModel(expectedReceivers, null);
        Collections.sort(expect4.getRecipients());
        Collections.sort(receivers4.getRecipients());
        Assert.assertEquals(expect4, receivers4);
    }



    @Test
    public void testEmptyQueryRecipients() throws Exception {
        configService.stopAlertSystem(new ConfigModel(), 1);
        alert.setAlertType(ALERT_TYPE.MARK_INSTANCE_UP);
        EmailReceiverModel result = policyManager.queryEmailReceivers(alert);
        logger.info("[recipients] {}", result);
        Assert.assertFalse(result.getRecipients().isEmpty());

        alert.setAlertType(ALERT_TYPE.ALERT_SYSTEM_OFF);
        result = policyManager.queryEmailReceivers(alert);
        logger.info("[recipients] {}", result);
        Assert.assertFalse(result.getRecipients().isEmpty());

        alert.setAlertType(ALERT_TYPE.SENTINEL_MONITOR_REDUNDANT_REDIS);
        result = policyManager.queryEmailReceivers(alert);
        logger.info("[recipients] {}", result);
        Assert.assertFalse(result.getRecipients().isEmpty());

        alert.setAlertType(ALERT_TYPE.CLIENT_INCONSIS);
        result = policyManager.queryEmailReceivers(alert);
        logger.info("[recipients] {}", result);
        Assert.assertFalse(result.getRecipients().isEmpty());
    }


    @Test
    public void testMarkCheckInterval() throws Exception {
        int checkInterval = randomInt();
        policyManager.markCheckInterval(ALERT_TYPE.MARK_INSTANCE_UP, ()->checkInterval);
        alert.setAlertType(ALERT_TYPE.MARK_INSTANCE_UP);
        Assert.assertEquals(new NaiveRecoveryTimeAlgorithm().calculate(checkInterval), policyManager.queryRecoverMilli(alert));
    }

    @Test
    public void testQueryGroupedEmailReceivers() throws Exception {
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

        Map<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> expect = Maps.newHashMap();

        Map<ALERT_TYPE, Set<AlertEntity>> dbaMap = Maps.newHashMap();
        dbaMap.put(ALERT_TYPE.CLIENT_INCONSIS, alerts.get(ALERT_TYPE.CLIENT_INCONSIS));
        dbaMap.put(ALERT_TYPE.XREDIS_VERSION_NOT_VALID, alerts.get(ALERT_TYPE.XREDIS_VERSION_NOT_VALID));
        dbaMap.put(ALERT_TYPE.REDIS_CONF_REWRITE_FAILURE, alerts.get(ALERT_TYPE.REDIS_CONF_REWRITE_FAILURE));
        dbaMap.put(ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR, alerts.get(ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR));

        Map<ALERT_TYPE, Set<AlertEntity>> xpipeAdminMap = Maps.newHashMap();
        xpipeAdminMap.put(ALERT_TYPE.QUORUM_DOWN_FAIL, alerts.get(ALERT_TYPE.QUORUM_DOWN_FAIL));
        xpipeAdminMap.put(ALERT_TYPE.SENTINEL_RESET, alerts.get(ALERT_TYPE.SENTINEL_RESET));
        xpipeAdminMap.put(ALERT_TYPE.MIGRATION_MANY_UNFINISHED, alerts.get(ALERT_TYPE.MIGRATION_MANY_UNFINISHED));

        expect.put(new EmailReceiverModel(Lists.newArrayList(consoleConfig.getDBAEmails()),
                Lists.newArrayList(consoleConfig.getXPipeAdminEmails())), dbaMap);

        expect.put(new EmailReceiverModel(Lists.newArrayList(consoleConfig.getXPipeAdminEmails()), null), xpipeAdminMap);

        Assert.assertEquals(expect, policyManager.queryGroupedEmailReceivers(convertToHolderManager(alerts)));

        configService.stopAlertSystem(new ConfigModel(), 1);

        expect.clear();
        expect.put(new EmailReceiverModel(Lists.newArrayList(consoleConfig.getXPipeAdminEmails()), null), alerts);
        Assert.assertEquals(expect, policyManager.queryGroupedEmailReceivers(convertToHolderManager(alerts)));

    }

    @After
    public void afterAlertPolicyManagerTest() throws DalException {
        configService.startAlertSystem(new ConfigModel());
    }

    private AlertEntityHolderManager convertToHolderManager(Map<ALERT_TYPE, Set<AlertEntity>> alerts) {
        AlertEntityHolderManager holderManager = new DefaultAlertEntityHolderManager();
        for(Map.Entry<ALERT_TYPE, Set<AlertEntity>> entry : alerts.entrySet()) {
            holderManager.bulkInsert(Lists.newArrayList(entry.getValue()));
        }
        return holderManager;
    }
}