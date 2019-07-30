package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.ConsoleConfigListener;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.DcClusterDelayMarkDown;
import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.*;

/**
 * @author shyin
 *         <p>
 *         Oct 19, 2016
 */
public class DefaultConsoleConfig extends AbstractCoreConfig implements ConsoleConfig {

    public static final String KEY_DATASOURCE = "datasource";
    public static final String KEY_CONSOLE_NOTIFY_RETRY_TIMES = "console.notify.retry.times";
    public static final String KEY_CONSOLE_NOTIFY_THREADS = "console.notify.threads";
    public static final String KEY_CONSOLE_NOTIFY_RETRY_INTERVAL = "console.notify.retry.interval";
    public static final String KEY_METASERVERS = "metaservers";
    public static final String KEY_USER_ACCESS_WHITE_LIST = "user.access.white.list";
    public static final String KEY_REDIS_REPLICATION_HEALTH_CHECK_INTERVAL = "redis.replication.health.check.interval";
    public static final String KEY_REDIS_CONF_CHECK_INTERVAL = "redis.conf.check.interval";
    public static final String KEY_HICKWALL_ADDRESS = "console.hickwall.address";
    public static final String KEY_HEALTHY_DELAY = "console.healthy.delay";
    public static final String KEY_DOWN_AFTER_CHECK_NUMS = "console.down.after.checknums";
    public static final String KEY_CACHE_REFERSH_INTERVAL = "console.cache.refresh.interval";
    public static final String KEY_ALERT_WHITE_LIST = "console.alert.whitelist";
    public static final String KEY_ALL_CONSOLES = "console.all.addresses";
    public static final String KEY_QUORUM = "console.quorum";
    public static final String KEY_DOMAIN = "console.domain";
    public static final String KEY_CNAME_TODC = "console.cname.todc";

    public static final String KEY_SENTINEL_QUORUM = "console.sentinel.quorum";

    private static final String KEY_REDIS_REPL_DISKLESS_MINIMUM_VERSION = "redis.repl.diskless.minimum.version";
    private static final String KEY_XREDIS_REQUEST_MINI_VERSION = "xredis.minimum.request.version";

    private static final String KEY_DBA_EMAILS = "redis.dba.emails";
    private static final String KEY_REDIS_ALERT_SENDER_EMAIL = "redis.alert.sender.email";
    private static final String KEY_XPIPE_RUNTIME_ENVIRONMENT = "xpipe.runtime.environment";
    private static final String KEY_XPIPE_ADMIN_EMAILS = "xpipe.admin.emails";

    private static final String KEY_ALERT_MESSAGE_SUSPEND_TIME = "alert.message.suspend.time";

    private static final String KEY_ALERT_MESSAGE_RECOVER_TIME = "alert.message.recover.time";

    private static final String KEY_CONFIG_DEFAULT_RESTORE_HOUR = "console.config.default.restore.hour";

    private static final String KEY_REBALANCE_SENTINEL_INTERVAL = "rebalance.sentinel.interval.second";

    private static final String KEY_REBALANCE_SENTINEL_MAX_NUM_ONCE = "rebalance.sentinel.max.num.once";

    private static final String KEY_NO_ALARM_MUNITE_FOR_NEW_CLUSTER = "no.alarm.minute.for.new.cluster";

    public static final String KEY_IGNORED_DC_FOR_HEALTH_CHECK = "ignored.dc.for.health.check";

    public static final String KEY_DC_CLUSTER_WONT_MARK_DOWN = "console.dc.cluster.pairs.delay.mark.down";

    private static final String KEY_HEALTHY_DELAY_THROUGH_PROXY = "console.healthy.delay.through.proxy";

    private static final String KEY_DOWN_AFTER_CHECK_NUMS_THROUGH_PROXY = "console.down.after.checknums.through.proxy";

    private static final String KEY_PING_DOWN_AFTER_MILLI = "console.ping.down.after.milli";

    private static final String KEY_PING_DOWN_AFTER_MILLI_THROUGH_PROXY = "console.ping.down.after.milli.through.proxy";

    private static final String KEY_DEFAULT_MARK_DOWN_DELAY_SEC = "console.default.mark.down.delay.sec";

    public static final String KEY_SOCKET_STATS_ANALYZERS = "console.socket.stats.analyzers";

    public static final String KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK = "console.cluster.shard.for.migrate.sys.check";

    private static final String KEY_DATABASE_DOMAIN_NAME = "console.database.domain.name";

    private static final String KEY_DATABASE_IP_ADDRESSES = "console.database.ip.address";

    private Map<String, List<ConsoleConfigListener>> listeners = Maps.newConcurrentMap();

    @Override
    public int getAlertSystemRecoverMinute() {
        return getIntProperty(KEY_ALERT_MESSAGE_RECOVER_TIME, 5);
    }

    @Override
    public int getAlertSystemSuspendMinute() {
        return getIntProperty(KEY_ALERT_MESSAGE_SUSPEND_TIME, 30);
    }

    @Override
    public String getDBAEmails() {
        return getProperty(KEY_DBA_EMAILS, "DBA@email.com");
    }

    @Override
    public String getRedisAlertSenderEmail() {
        return getProperty(KEY_REDIS_ALERT_SENDER_EMAIL, "");
    }

    @Override
    public String getXpipeRuntimeEnvironmentEnvironment() {
        return getProperty(KEY_XPIPE_RUNTIME_ENVIRONMENT, "");
    }

    @Override
    public String getXPipeAdminEmails() {
        return getProperty(KEY_XPIPE_ADMIN_EMAILS, "XPipeAdmin@email.com");
    }

    @Override
    public String getReplDisklessMinRedisVersion() {
        return getProperty(KEY_REDIS_REPL_DISKLESS_MINIMUM_VERSION, "2.8.22");
    }

    @Override
    public String getXRedisMinimumRequestVersion() {
        return getProperty(KEY_XREDIS_REQUEST_MINI_VERSION, "0.0.3");
    }

    @Override
    public String getDatasource() {
        return getProperty(KEY_DATASOURCE, "fxxpipe");
    }

    @Override
    public int getConsoleNotifyRetryTimes() {
        return getIntProperty(KEY_CONSOLE_NOTIFY_RETRY_TIMES, 3);
    }

    @Override
    public int getConsoleNotifyRetryInterval() {
        return getIntProperty(KEY_CONSOLE_NOTIFY_RETRY_INTERVAL, 100);
    }

    @Override
    public String getMetaservers() {
        return getProperty(KEY_METASERVERS, "{}");
    }

    @Override
    public int getConsoleNotifyThreads() {
        return getIntProperty(KEY_CONSOLE_NOTIFY_THREADS, 20);
    }

    @Override
    public Set<String> getConsoleUserAccessWhiteList() {
        String whiteList = getProperty(KEY_USER_ACCESS_WHITE_LIST, "*");
        return new HashSet<>(Arrays.asList(whiteList.split(",")));
    }

    @Override
    public int getRedisReplicationHealthCheckInterval() {
        return getIntProperty(KEY_REDIS_REPLICATION_HEALTH_CHECK_INTERVAL, 2000);
    }

    @Override
    public String getHickwallAddress() {
        return getProperty(KEY_HICKWALL_ADDRESS, "http://hickwall.qa.nt.ctripcorp.com/grafana/dashboard/script/scripted_sole.js?from=now-15m&to=now&target=");
    }

    @Override
    public int getHealthyDelayMilli() {
        return getIntProperty(KEY_HEALTHY_DELAY, 2000);
    }

    @Override
    public int getHealthyDelayMilliThroughProxy() {
        return getIntProperty(KEY_HEALTHY_DELAY_THROUGH_PROXY, 30 * 1000);
    }

    @Override
    public int getDownAfterCheckNums() {
        return getIntProperty(KEY_DOWN_AFTER_CHECK_NUMS, 5);
    }

    @Override
    public int getDownAfterCheckNumsThroughProxy() {
        return getIntProperty(KEY_DOWN_AFTER_CHECK_NUMS_THROUGH_PROXY, 10);
    }

    @Override
    public int getCacheRefreshInterval() {
        return getIntProperty(KEY_CACHE_REFERSH_INTERVAL, 1000);
    }

    @Override
    public Set<String> getAlertWhileList() {
        String whitelist = getProperty(KEY_ALERT_WHITE_LIST, "");

        return getSplitStringSet(whitelist);

    }

    private Set<String> getSplitStringSet(String str) {
        HashSet result = new HashSet();

        String[] split = str.split("\\s*(,|;)\\s*");

        for(String sp : split){
            if(!StringUtil.isEmpty(sp)){
                result.add(sp);
            }
        }
        return result;
    }

    @Override
    public String getAllConsoles() {
        return getProperty(KEY_ALL_CONSOLES, "127.0.0.1:8080");
    }

    @Override
    public int getQuorum() {
        return getIntProperty(KEY_QUORUM, 1);
    }

    @Override
    public int getRedisConfCheckIntervalMilli() {

        return getIntProperty(
                KEY_REDIS_CONF_CHECK_INTERVAL,
                Integer.parseInt(System.getProperty(KEY_REDIS_CONF_CHECK_INTERVAL, "300000"))
        );
    }

    @Override
    public String getConsoleDomain() {
        return getProperty(KEY_DOMAIN, "127.0.0.1");
    }

    @Override
    public Map<String, String> getConsoleCnameToDc() {

        String property = getProperty(KEY_CNAME_TODC, "{}");
        return JsonCodec.INSTANCE.decode(property, Map.class);
    }

    @Override
    public QuorumConfig getDefaultSentinelQuorumConfig() {

        String config = getProperty(KEY_SENTINEL_QUORUM, "{}");
        return JsonCodec.INSTANCE.decode(config, QuorumConfig.class);
    }

    @Override
    public int getConfigDefaultRestoreHours() {
        return getIntProperty(KEY_CONFIG_DEFAULT_RESTORE_HOUR, 10);
    }

    @Override
    public int getRebalanceSentinelInterval() {
        return getIntProperty(KEY_REBALANCE_SENTINEL_INTERVAL, 120);
    }

    @Override
    public int getRebalanceSentinelMaxNumOnce() {
        return getIntProperty(KEY_REBALANCE_SENTINEL_MAX_NUM_ONCE, 15);
    }

    @Override
    public int getNoAlarmMinutesForNewCluster() {
        return getIntProperty(KEY_NO_ALARM_MUNITE_FOR_NEW_CLUSTER, 15);
    }

    @Override
    public Set<String> getIgnoredHealthCheckDc() {
        return getSplitStringSet(getProperty(KEY_IGNORED_DC_FOR_HEALTH_CHECK, ""));
    }

    private int getDefaultMarkDownDelaySecond() {
        return getIntProperty(KEY_DEFAULT_MARK_DOWN_DELAY_SEC, 60 * 60);
    }

    @Override
    public Set<DcClusterDelayMarkDown> getDelayedMarkDownDcClusters() {
        Set<DcClusterDelayMarkDown> result = Sets.newHashSet();
        Set<String> dcClusters = getSplitStringSet(getProperty(KEY_DC_CLUSTER_WONT_MARK_DOWN, ""));
        for(String dcCluster : dcClusters) {
            String[] pair = StringUtil.splitRemoveEmpty("\\s*:\\s*", dcCluster);
            DcClusterDelayMarkDown instance = new DcClusterDelayMarkDown().setDcId(pair[0]).setClusterId(pair[1]);
            int delaySec = pair.length > 2 ? Integer.parseInt(pair[2]) : getDefaultMarkDownDelaySecond();
            result.add(instance.setDelaySecond(delaySec));
        }
        return result;
    }

    @Override
    public int getPingDownAfterMilli() {
        return getIntProperty(KEY_PING_DOWN_AFTER_MILLI, 12 * 1000);
    }

    @Override
    public int getPingDownAfterMilliThroughProxy() {
        return getIntProperty(KEY_PING_DOWN_AFTER_MILLI_THROUGH_PROXY, 30 * 1000);
    }

    @Override
    public void onChange(String key, String oldValue, String newValue) {
        super.onChange(key, oldValue, newValue);
        if(!listeners.containsKey(key)) {
            return;
        }
        for(ConsoleConfigListener listener : listeners.get(key)) {
            listener.onChange(key, oldValue, newValue);
        }
    }

    @Override
    public void register(ConsoleConfigListener consoleConfigListener) {
        for(String key : consoleConfigListener.supportsKeys()) {
            listeners.putIfAbsent(key, new LinkedList<>());
            listeners.get(key).add(consoleConfigListener);
        }
    }

    @Override
    public Map<String, String> getSocketStatsAnalyzingKeys() {
        String property = getProperty(KEY_SOCKET_STATS_ANALYZERS, "{}");
        return JsonCodec.INSTANCE.decode(property, Map.class);
    }

    @Override
    public Pair<String, String> getClusterShardForMigrationSysCheck() {
        String clusterShard = getProperty(KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK, "cluster1, shard1");
        String[] strs = StringUtil.splitRemoveEmpty("\\s*,\\s*", clusterShard);
        return Pair.from(strs[0], strs[1]);
    }

    @Override
    public String getDatabaseDomainName() {
        return getProperty(KEY_DATABASE_DOMAIN_NAME, "localhost");
    }

    @Override
    public Map<String, String> getDatabaseIpAddresses() {
        String property = getProperty(KEY_DATABASE_IP_ADDRESSES, "{}");
        return JsonCodec.INSTANCE.decode(property, Map.class);
    }
}
