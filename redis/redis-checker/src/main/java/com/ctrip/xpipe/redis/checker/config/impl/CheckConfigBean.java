package com.ctrip.xpipe.redis.checker.config.impl;

import com.ctrip.xpipe.api.config.ConfigProvider;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import java.util.*;

@Configuration
public class CheckConfigBean extends AbstractConfigBean {

    public static final String KEY_MAX_REMOVED_DCS_CNT = "max.removed.dcs.count";

    public static final String KEY_MAX_REMOVED_CLUSTERS_PERCENT = "max.removed.clusters.percent";

    public static final String KEY_QUORUM = "console.quorum";

    public static final String KEY_DOWN_AFTER_CHECK_NUMS = "console.down.after.checknums";

    public static final String KEY_DOWN_AFTER_CHECK_NUMS_THROUGH_PROXY = "console.down.after.checknums.through.proxy";

    public static final String KEY_PING_DOWN_AFTER_MILLI_THROUGH_PROXY = "console.ping.down.after.milli.through.proxy";

    public static final String KEY_PING_DOWN_AFTER_MILLI = "console.ping.down.after.milli";

    public static final String KEY_HEALTHY_DELAY_THROUGH_PROXY = "console.healthy.delay.through.proxy";

    public static final String KEY_HEALTHY_DELAY = "console.healthy.delay";

    public static final String KEY_REDIS_REPL_DISKLESS_MINIMUM_VERSION = "redis.repl.diskless.minimum.version";

    public static final String KEY_REDIS_REPLICATION_HEALTH_CHECK_INTERVAL = "redis.replication.health.check.interval";

    public static final String KEY_XREDIS_REQUEST_MINI_VERSION = "xredis.minimum.request.version";

    public static final String KEY_REDIS_CONF_CHECK_INTERVAL = "redis.conf.check.interval";

    public static final String KEY_CREDIS_CLUSTER_REFRESH_INTERVAL = "credis.cluster.refresh.interval";

    public static final String KEY_SENTINEL_QUORUM = "console.sentinel.quorum";

    public static final String KEY_SENTINEL_CHECK_INTERVAL = "console.health.sentinel.interval";

    public static final String KEY_SENTINEL_RATE_LIMIT_OPEN = "console.sentinel.rate.limit.open";

    public static final String KEY_SENTINEL_RATE_LIMIT_SIZE = "console.sentinel.rate.limit.size";

    public static final String KEY_CROSS_DC_SENTINEL_MONITOR_NAME_SUFFIX = "checker.cross.dc.sentinel.monitor.name.suffix";

    public static final String KEY_SENTINEL_MASTER_CONFIG = "checker.sentinel.master.config";

    public static final String KEY_SHOULD_SENTINEL_CHECK_OUTER_TYPES = "console.should.sentinel.check.outer.types";

    public static final String KEY_SENTINEL_CHECK_OUTER_CLUSTERS = "console.sentinel.check.outer.clusters";

    public static final String KEY_CLUSTERS_LIST = "checker.clusters.list";

    public static final String KEY_CONSOLE_ADDRESS = "console.address";

    public static final String KEY_CHECKER_META_REFRESH_INTERVAL = "checker.meta.refresh.interval.milli";

    public static final String KEY_CHECKER_REPORT_INTERVAL = "checker.report.interval.milli";

    public static final String KEY_REDIS_CONFIG_CHECK_MONITOR_OPEN = "cosnole.redis.config.check.open";

    public static final String KEY_REDIS_CONFIG_CHECK_RULES = "console.redis.config.check.rules";

    public static final String KEY_CLUSTER_HEALTH_CHECK_INTERVAL = "cluster.health.check.interval";

    public static final String KEY_OUTTER_CLIENT_CHECK_INTERVAL = "console.outter.client.check.interval";

    public static final String KEY_HEALTHY_MARK_COMPENSATE_INTERVAL_MILLI = "console.health.compensate.interval.milli";

    public static final String KEY_HEALTHY_MARK_COMPENSATE_THREADS = "console.health.compensate.threads";

    public static final String KEY_SENTINEL_CHECK_DOWNGRADE_STRATEGY = "checker.sentinel.check.downgrade.strategy";

    public static final String KEY_INSTANCE_LONG_DELAY_MILLI = "console.instance.long.delay.milli";

    public static final String KEY_CHECKER_SITE_STABLE = "checker.stable";

    public static final String KEY_CHECKER_STABLE_RECOVER_AFTER_ROUNDS = "checker.stable.recover.after.rounds";

    public static final String KEY_CHECKER_STABLE_RESET_AFTER_ROUNDS = "checker.stable.reset.after.rounds";

    public static final String KEY_CHECKER_STABLE_LOSS_AFTER_ROUNDS = "checker.stable.loss.after.rounds";

    public static final String KEY_CHECKER_STABLE_THRESHOLD = "checker.stable.recover.threshold";

    public static final String KEY_CHECKER_UNSTABLE_THRESHOLD = "checker.stable.loss.threshold";

    public static final String KEY_IGNORED_DC_FOR_HEALTH_CHECK = "ignored.dc.for.health.check";

    public static final String KEY_CHECKER_CURRENT_DC_ALL_META_REFRESH_INTERVAL = "checker.current_dc_all_meta.refresh.interval.milli";

    public static final String KEY_CHECKER_ACK_INTERVAL = "checker.ack.interval.milli";

    public static final String KEY_CONFIG_CACHE_TIMEOUT_MILLI = "checker.config.cache.timeout.milli";

    public static final String KEY_PROXY_CHECK_DOWN_RETRY_TIMES = "proxy.check.down.retry.times";

    public static final String KEY_PROXY_CHECK_UP_RETRY_TIMES = "proxy.check.up.retry.times";

    public static final String KEY_PROXY_CHECK_INTERVAL = "proxy.check.interval";

    public static final String KEY_NON_CORE_CHECK_INTERVAL = "non.core.check.interval";

    public static final String KEY_SUBSCRIBE_TIMEOUT_MILLI = "checker.subscribe.timeout.milli";

    public static final String KEY_KEEPER_CHECKER_INTERVAL = "keeper.checker.interval";

    public static final String KEY_CHECKER_MARK_DELAY_BASE = "checker.health.mark.delay.base.milli";

    public static final String KEY_CHECKER_MARK_DELAY_MAX = "checker.health.mark.delay.max.milli";

    public static final String KEY_CHECKER_MARK_UP_DELAY_MAX = "checker.health.mark.up.delay.max.milli";

    public static final String KEY_CHECKER_INSTANCE_PULL_INTERVAL = "checker.instance.pull.interval";

    public static final String KEY_CHECKER_INSTANCE_PULL_RANDOM = "checker.instance.pull.random";

    private FoundationService foundationService;

    @Autowired
    public CheckConfigBean(FoundationService foundationService) {
        super(ConfigProvider.DEFAULT.getOrCreateConfig(ConfigProvider.CHECK_CONFIG_NAME));
        this.foundationService = foundationService;
    }

    public int maxRemovedDcsCnt() {
        return getIntProperty(KEY_MAX_REMOVED_DCS_CNT, 1);
    }

    public int maxRemovedClustersPercent() {
        return getIntProperty(KEY_MAX_REMOVED_CLUSTERS_PERCENT, 50);
    }

    public int getQuorum() {
        return getIntProperty(KEY_QUORUM, 1);
    }

    public int getDownAfterCheckNums() {
        return getIntProperty(KEY_DOWN_AFTER_CHECK_NUMS, 5);
    }

    public int getDownAfterCheckNumsThroughProxy() {
        return getIntProperty(KEY_DOWN_AFTER_CHECK_NUMS_THROUGH_PROXY, 10);
    }

    public int getPingDownAfterMilliThroughProxy() {
        return getIntProperty(KEY_PING_DOWN_AFTER_MILLI_THROUGH_PROXY, 30 * 1000);
    }

    public int getPingDownAfterMilli() {
        return getIntProperty(KEY_PING_DOWN_AFTER_MILLI, 12 * 1000);
    }

    public int getHealthyDelayMilliThroughProxy() {
        return getIntProperty(KEY_HEALTHY_DELAY_THROUGH_PROXY, 30 * 1000);
    }

    public int getHealthyDelayMilli() {
        return getIntProperty(KEY_HEALTHY_DELAY, 2000);
    }

    public String getReplDisklessMinRedisVersion() {
        return getProperty(KEY_REDIS_REPL_DISKLESS_MINIMUM_VERSION, "2.8.22");
    }

    public int getRedisReplicationHealthCheckInterval() {
        return getIntProperty(KEY_REDIS_REPLICATION_HEALTH_CHECK_INTERVAL, 2000);
    }

    public String getXRedisMinimumRequestVersion() {
        return getProperty(KEY_XREDIS_REQUEST_MINI_VERSION, "0.0.3");
    }

    public int getRedisConfCheckIntervalMilli() {

        return getIntProperty(
                KEY_REDIS_CONF_CHECK_INTERVAL,
                Integer.parseInt(System.getProperty(KEY_REDIS_CONF_CHECK_INTERVAL, "300000"))
        );
    }

    public int getCRedisClusterRefreshIntervalMilli() {
        return getIntProperty(KEY_CREDIS_CLUSTER_REFRESH_INTERVAL, 60000);
    }

    public QuorumConfig getDefaultSentinelQuorumConfig() {

        String config = getProperty(KEY_SENTINEL_QUORUM, "{}");
        return JsonCodec.INSTANCE.decode(config, QuorumConfig.class);
    }

    public int getSentinelCheckIntervalMilli() {
        return getIntProperty(KEY_SENTINEL_CHECK_INTERVAL, 300000);
    }

    public boolean isSentinelRateLimitOpen() {
        return getBooleanProperty(KEY_SENTINEL_RATE_LIMIT_OPEN, false);
    }

    public int getSentinelRateLimitSize() {
        return getIntProperty(KEY_SENTINEL_RATE_LIMIT_SIZE, 3);
    }

    public String crossDcSentinelMonitorNameSuffix() {
        return getProperty(KEY_CROSS_DC_SENTINEL_MONITOR_NAME_SUFFIX, "CROSS_DC");
    }

    public Map<String, String> sentinelMasterConfig() {
        String property = getProperty(KEY_SENTINEL_MASTER_CONFIG, "{}");
        return JsonCodec.INSTANCE.decode(property, Map.class);
    }

    public boolean shouldSentinelCheckOuterClientClusters() {
        return getBooleanProperty(KEY_SHOULD_SENTINEL_CHECK_OUTER_TYPES, false);
    }

    public Set<String> sentinelCheckOuterClientClusters() {
        return getSplitStringSet(getProperty(KEY_SENTINEL_CHECK_OUTER_CLUSTERS, "").toLowerCase());
    }

    public int getClustersPartIndex() {
        String groupList = getProperty(KEY_CLUSTERS_LIST, "");
        String[] strs = StringUtil.splitRemoveEmpty("\\s*,\\s*", groupList);
        int res = Arrays.asList(strs).indexOf(foundationService.getGroupId());
        if(res == -1) {
            logger.error("groupId[{}] not found", foundationService.getGroupId());
        }
        return res;
    }

    public String getServerMode() {

        List<String> clustersList = getClustersList();
        int res = clustersList.indexOf(foundationService.getGroupId());
        if(res == -1) {
            return ConsoleServerModeCondition.SERVER_MODE.CONSOLE.name();
        } else {
            return ConsoleServerModeCondition.SERVER_MODE.CHECKER.name();
        }
    }

    public List<String> getClustersList() {
        String groupList = getProperty(KEY_CLUSTERS_LIST, "");
        String[] strs = StringUtil.splitRemoveEmpty("\\s*,\\s*", groupList);
        if(strs.length == 0) {
            return new ArrayList<>();
        }
        return Arrays.asList(strs);
    }

    public String getConsoleAddress() {
        return getProperty(KEY_CONSOLE_ADDRESS, "http://localhost:8080");
    }

    public int getCheckerMetaRefreshIntervalMilli() {
        return getIntProperty(KEY_CHECKER_META_REFRESH_INTERVAL, 30000);
    }

    public int getCheckerReportIntervalMilli() {
        return getIntProperty(KEY_CHECKER_REPORT_INTERVAL, 10000);
    }

    public boolean isRedisConfigCheckMonitorOpen() {
        return getBooleanProperty(KEY_REDIS_CONFIG_CHECK_MONITOR_OPEN, false);
    }

    public String getRedisConfigCheckRules() {
        return getProperty(KEY_REDIS_CONFIG_CHECK_RULES);
    }

    public int getClusterHealthCheckInterval() {
        return getIntProperty(KEY_CLUSTER_HEALTH_CHECK_INTERVAL, 300000);
    }

    public int getOutterClientCheckInterval() {
        return getIntProperty(KEY_OUTTER_CLIENT_CHECK_INTERVAL, 120 * 1000);
    }

    public long getHealthMarkCompensateIntervalMill() {
        return getLongProperty(KEY_HEALTHY_MARK_COMPENSATE_INTERVAL_MILLI, 2 * 60 * 1000L);
    }

    public int getHealthMarkCompensateThreads() {
        return getIntProperty(KEY_HEALTHY_MARK_COMPENSATE_THREADS, 20);
    }

    public String sentinelCheckDowngradeStrategy() {
        return getProperty(KEY_SENTINEL_CHECK_DOWNGRADE_STRATEGY, "lessThanHalf");
    }

    public int getInstanceLongDelayMilli() {
        return getIntProperty(KEY_INSTANCE_LONG_DELAY_MILLI, 3 * 60 * 1000);
    }

    public int getStableLossAfterRounds() {
        return getIntProperty(KEY_CHECKER_STABLE_LOSS_AFTER_ROUNDS, 10);
    }

    public Boolean getSiteStable() {
        return getBooleanProperty(KEY_CHECKER_SITE_STABLE, null);
    }

    public int getStableRecoverAfterRounds() {
        return getIntProperty(KEY_CHECKER_STABLE_RECOVER_AFTER_ROUNDS, 30 * 30);
    }

    public float getSiteStableThreshold() {
        return getFloatProperty(KEY_CHECKER_STABLE_THRESHOLD, 0.8f);
    }

    public float getSiteUnstableThreshold() {
        return getFloatProperty(KEY_CHECKER_UNSTABLE_THRESHOLD, 0.8f);
    }

    public Set<String> getIgnoredHealthCheckDc() {
        return getSplitStringSet(getProperty(KEY_IGNORED_DC_FOR_HEALTH_CHECK, ""));
    }

    public int getCheckerCurrentDcAllMetaRefreshIntervalMilli() {
        return getIntProperty(KEY_CHECKER_CURRENT_DC_ALL_META_REFRESH_INTERVAL, 60 * 1000);
    }

    public int getCheckerAckIntervalMilli() {
        return getIntProperty(KEY_CHECKER_ACK_INTERVAL, 10000);
    }

    public long getConfigCacheTimeoutMilli() {
        return getLongProperty(KEY_CONFIG_CACHE_TIMEOUT_MILLI, 1000L);
    }

    public int getProxyCheckDownRetryTimes() {
        return getIntProperty(KEY_PROXY_CHECK_DOWN_RETRY_TIMES, 1);
    }

    public int getProxyCheckUpRetryTimes() {
        return getIntProperty(KEY_PROXY_CHECK_UP_RETRY_TIMES, 10);
    }

    public int getNonCoreCheckIntervalMilli() {
        return getIntProperty(KEY_NON_CORE_CHECK_INTERVAL, 3 * 60 * 60 * 1000);
    }

    public long subscribeTimeoutMilli() {
        return getLongProperty(KEY_SUBSCRIBE_TIMEOUT_MILLI, 5000L);
    }

    public int getKeeperCheckerIntervalMilli() {
        return getIntProperty(KEY_KEEPER_CHECKER_INTERVAL, 60 * 1000);
    }

    public int getMarkInstanceBaseDelayMilli() {
        return getIntProperty(KEY_CHECKER_MARK_DELAY_BASE, 500);
    }

    public int getMarkdownInstanceMaxDelayMilli() {
        return getIntProperty(KEY_CHECKER_MARK_DELAY_MAX, 14000);
    }

    public int getMarkupInstanceMaxDelayMilli() {
        return getIntProperty(KEY_CHECKER_MARK_UP_DELAY_MAX, 60000);
    }

    public int getInstancePullIntervalSeconds() {
        return getIntProperty(KEY_CHECKER_INSTANCE_PULL_INTERVAL, 5);
    }

    public int getInstancePullRandomSeconds() {
        return getIntProperty(KEY_CHECKER_INSTANCE_PULL_RANDOM, 5);
    }

    public int getStableResetAfterRounds() {
        return getIntProperty(KEY_CHECKER_STABLE_RESET_AFTER_ROUNDS, 30);
    }

}
