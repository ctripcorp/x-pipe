package com.ctrip.xpipe.redis.console.alert.policy.receiver;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.message.AlertEntityHolderManager;
import com.ctrip.xpipe.redis.console.alert.message.holder.DefaultAlertEntityHolderManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Apr 20, 2018
 */
public class DefaultGroupEmailReceiverTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private ConfigService configService;

    @Autowired
    private ClusterService clusterService;

    private DefaultGroupEmailReceiver groupEmailReceiver;

    private AlertEntity alert;

    @Before
    public void beforeDefaultGroupEmailReceiverTest() {
        groupEmailReceiver = new DefaultGroupEmailReceiver(consoleConfig, configService, clusterService);

        alert = new AlertEntity(new HostPort("192.168.1.10", 6379), dcNames[0],
                "clusterId", "shardId", "test message", ALERT_TYPE.XREDIS_VERSION_NOT_VALID);
    }

    @Test
    public void getGroupedEmailReceiver() throws DalException {
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

        Assert.assertEquals(expect, groupEmailReceiver.getGroupedEmailReceiver(convertToHolderManager(alerts)));

        configService.stopAlertSystem(new ConfigModel(), 1);

        expect.clear();
        expect.put(new EmailReceiverModel(Lists.newArrayList(consoleConfig.getXPipeAdminEmails()), null), alerts);
        Assert.assertEquals(expect, groupEmailReceiver.getGroupedEmailReceiver(convertToHolderManager(alerts)));
    }

    private AlertEntityHolderManager convertToHolderManager(Map<ALERT_TYPE, Set<AlertEntity>> alerts) {
        AlertEntityHolderManager holderManager = new DefaultAlertEntityHolderManager();
        for(Map.Entry<ALERT_TYPE, Set<AlertEntity>> entry : alerts.entrySet()) {
            holderManager.bulkInsert(Lists.newArrayList(entry.getValue()));
        }
        return holderManager;
    }

    @Test
    public void getGroupedEmailReceiver2() throws DalException {
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
        alerts.put(ALERT_TYPE.REDIS_CONF_REWRITE_FAILURE,
                Collections.singleton(
                        new AlertEntity(hostPort, dcNames[0], "cluster-test", "shard-test", "", ALERT_TYPE.REDIS_CONF_REWRITE_FAILURE
                        )));
        alerts.put(ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR,
                Collections.singleton(
                        new AlertEntity(hostPort, dcNames[0], "cluster-test", "shard-test", "", ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR
                        )));

        Map<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> expect = Maps.newHashMap();

        Map<ALERT_TYPE, Set<AlertEntity>> dbaMap = Maps.newHashMap();
        dbaMap.put(ALERT_TYPE.CLIENT_INCONSIS, alerts.get(ALERT_TYPE.CLIENT_INCONSIS));
        dbaMap.put(ALERT_TYPE.XREDIS_VERSION_NOT_VALID, alerts.get(ALERT_TYPE.XREDIS_VERSION_NOT_VALID));
        dbaMap.put(ALERT_TYPE.REDIS_CONF_REWRITE_FAILURE, alerts.get(ALERT_TYPE.REDIS_CONF_REWRITE_FAILURE));
        dbaMap.put(ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR, alerts.get(ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR));

        Map<ALERT_TYPE, Set<AlertEntity>> emptyMap = Maps.newHashMap();

        expect.put(new EmailReceiverModel(Lists.newArrayList(consoleConfig.getDBAEmails()),
                Lists.newArrayList(consoleConfig.getXPipeAdminEmails())), dbaMap);

        expect.put(new EmailReceiverModel(Lists.newArrayList(consoleConfig.getXPipeAdminEmails()), null), emptyMap);

        Assert.assertEquals(expect, groupEmailReceiver.getGroupedEmailReceiver(convertToHolderManager(alerts)));

        configService.stopAlertSystem(new ConfigModel(), 1);

        expect.clear();
        expect.put(new EmailReceiverModel(Lists.newArrayList(consoleConfig.getXPipeAdminEmails()), null), alerts);
        Assert.assertEquals(expect, groupEmailReceiver.getGroupedEmailReceiver(convertToHolderManager(alerts)));
    }


    @Test
    public void getGroupedEmailReceiver3() throws DalException {
        HostPort hostPort = new HostPort("192.168.1.10", 6379);
        Map<ALERT_TYPE, Set<AlertEntity>> alerts = new HashMap<>();

        alerts.put(ALERT_TYPE.QUORUM_DOWN_FAIL,
                Collections.singleton(
                        new AlertEntity(hostPort, dcNames[0], "cluster-test", "shard-test", "", ALERT_TYPE.QUORUM_DOWN_FAIL
                        )));
        alerts.put(ALERT_TYPE.SENTINEL_RESET,
                Collections.singleton(
                        new AlertEntity(hostPort, dcNames[0], "cluster-test", "shard-test", "", ALERT_TYPE.SENTINEL_RESET
                        )));
        alerts.put(ALERT_TYPE.MIGRATION_MANY_UNFINISHED,
                Collections.singleton(
                        new AlertEntity(hostPort, dcNames[0], "cluster-test", "shard-test", "", ALERT_TYPE.MIGRATION_MANY_UNFINISHED
                        )));

        Map<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> expect = Maps.newHashMap();

        Map<ALERT_TYPE, Set<AlertEntity>> emptyMap = Maps.newHashMap();

        Map<ALERT_TYPE, Set<AlertEntity>> xpipeAdminMap = Maps.newHashMap();
        xpipeAdminMap.put(ALERT_TYPE.QUORUM_DOWN_FAIL, alerts.get(ALERT_TYPE.QUORUM_DOWN_FAIL));
        xpipeAdminMap.put(ALERT_TYPE.SENTINEL_RESET, alerts.get(ALERT_TYPE.SENTINEL_RESET));
        xpipeAdminMap.put(ALERT_TYPE.MIGRATION_MANY_UNFINISHED, alerts.get(ALERT_TYPE.MIGRATION_MANY_UNFINISHED));

        expect.put(new EmailReceiverModel(Lists.newArrayList(consoleConfig.getDBAEmails()),
                Lists.newArrayList(consoleConfig.getXPipeAdminEmails())), emptyMap);

        expect.put(new EmailReceiverModel(Lists.newArrayList(consoleConfig.getXPipeAdminEmails()), null), xpipeAdminMap);

        Assert.assertEquals(expect, groupEmailReceiver.getGroupedEmailReceiver(convertToHolderManager(alerts)));

        configService.stopAlertSystem(new ConfigModel(), 1);

        expect.clear();
        expect.put(new EmailReceiverModel(Lists.newArrayList(consoleConfig.getXPipeAdminEmails()), null), alerts);
        Assert.assertEquals(expect, groupEmailReceiver.getGroupedEmailReceiver(convertToHolderManager(alerts)));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void receivers() {
        groupEmailReceiver.receivers(alert);
    }
}