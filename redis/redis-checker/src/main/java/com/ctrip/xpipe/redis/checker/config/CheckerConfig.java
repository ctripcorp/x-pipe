package com.ctrip.xpipe.redis.checker.config;

import com.ctrip.xpipe.api.config.ConfigChangeListener;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DcClusterDelayMarkDown;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/3/8
 */
public interface CheckerConfig {

    String KEY_CLUSTER_HEALTH_CHECK_INTERVAL = "cluster.health.check.interval";

    String KEY_REDIS_REPLICATION_HEALTH_CHECK_INTERVAL = "redis.replication.health.check.interval";

    String KEY_HEALTHY_DELAY = "console.healthy.delay";

    String KEY_HEALTHY_MARK_COMPENSATE_INTERVAL_MILLI = "console.health.compensate.interval.milli";

    String KEY_HEALTHY_MARK_COMPENSATE_THREADS = "console.health.compensate.threads";

    String KEY_REDIS_CONF_CHECK_INTERVAL = "redis.conf.check.interval";

    String KEY_DOWN_AFTER_CHECK_NUMS = "console.down.after.checknums";

    String KEY_REDIS_REPL_DISKLESS_MINIMUM_VERSION = "redis.repl.diskless.minimum.version";

    String KEY_XREDIS_REQUEST_MINI_VERSION = "xredis.minimum.request.version";

    String KEY_HEALTHY_DELAY_THROUGH_PROXY = "console.healthy.delay.through.proxy";

    String KEY_INSTANCE_LONG_DELAY_MILLI = "console.instance.long.delay.milli";

    String KEY_DOWN_AFTER_CHECK_NUMS_THROUGH_PROXY = "console.down.after.checknums.through.proxy";

    String KEY_PING_DOWN_AFTER_MILLI = "console.ping.down.after.milli";

    String KEY_PING_DOWN_AFTER_MILLI_THROUGH_PROXY = "console.ping.down.after.milli.through.proxy";

    String KEY_SENTINEL_CHECK_INTERVAL = "console.health.sentinel.interval";

    String KEY_SHOULD_SENTINEL_CHECK_OUTER_TYPES = "console.should.sentinel.check.outer.types";

    String KEY_SENTINEL_CHECK_OUTER_CLUSTERS = "console.sentinel.check.outer.clusters";

    String KEY_SENTINEL_CHECK_DOWNGRADE_STRATEGY = "checker.sentinel.check.downgrade.strategy";

    String KEY_SENTINEL_RATE_LIMIT_SIZE = "console.sentinel.rate.limit.size";

    String KEY_SENTINEL_RATE_LIMIT_OPEN = "console.sentinel.rate.limit.open";

    String KEY_SENTINEL_QUORUM = "console.sentinel.quorum";

    String KEY_DC_CLUSTER_WONT_MARK_DOWN = "console.dc.cluster.pairs.delay.mark.down";

    String KEY_DEFAULT_MARK_DOWN_DELAY_SEC = "console.default.mark.down.delay.sec";

    String KEY_CHECKER_SITE_STABLE = "checker.stable";

    String KEY_CHECKER_STABLE_RECOVER_AFTER_ROUNDS = "checker.stable.recover.after.rounds";

    String KEY_CHECKER_STABLE_RESET_AFTER_ROUNDS = "checker.stable.reset.after.rounds";

    String KEY_CHECKER_STABLE_LOSS_AFTER_ROUNDS = "checker.stable.loss.after.rounds";

    String KEY_CHECKER_STABLE_THRESHOLD = "checker.stable.recover.threshold";

    String KEY_CHECKER_UNSTABLE_THRESHOLD = "checker.stable.loss.threshold";

    String KEY_QUORUM = "console.quorum";

    String KEY_IGNORED_DC_FOR_HEALTH_CHECK = "ignored.dc.for.health.check";

    String KEY_CLUSTERS_PART_INDEX = "checker.clusters.part.index";

    String KEY_CHECKER_REPORT_INTERVAL = "checker.report.interval.milli";

    String KEY_CHECKER_META_REFRESH_INTERVAL = "checker.meta.refresh.interval.milli";

    String KEY_CHECKER_CURRENT_DC_ALL_META_REFRESH_INTERVAL = "checker.current_dc_all_meta.refresh.interval.milli";

    String KEY_CONSOLE_ADDRESS = "console.address";

    String KEY_CHECKER_ACK_INTERVAL = "checker.ack.interval.milli";

    String KEY_CHECKER_ADDRESS_ALL = "checker.address.all";

    String KEY_CONFIG_CACHE_TIMEOUT_MILLI = "checker.config.cache.timeout.milli";

    String KEY_PROXY_CHECK_DOWN_RETRY_TIMES = "proxy.check.down.retry.times";

    String KEY_PROXY_CHECK_UP_RETRY_TIMES = "proxy.check.up.retry.times";

    String KEY_PROXY_CHECK_INTERVAL = "proxy.check.interval";

    String KEY_NON_CORE_CHECK_INTERVAL = "non.core.check.interval";

    String KEY_SUBSCRIBE_TIMEOUT_MILLI = "checker.subscribe.timeout.milli";

    String KEY_KEEPER_CHECKER_INTERVAL = "keeper.checker.interval";

    int getRedisReplicationHealthCheckInterval();

    int getCheckerCurrentDcAllMetaRefreshIntervalMilli();

    int getClusterHealthCheckInterval();

    int getDownAfterCheckNums();

    int getDownAfterCheckNumsThroughProxy();

    int getRedisConfCheckIntervalMilli();

    int getSentinelCheckIntervalMilli();

    int getHealthyDelayMilli();

    long getHealthMarkCompensateIntervalMill();

    int getHealthMarkCompensateThreads();

    int getHealthyDelayMilliThroughProxy();

    int getInstanceLongDelayMilli();

    String getReplDisklessMinRedisVersion();

    String getXRedisMinimumRequestVersion();

    int getPingDownAfterMilli();

    int getPingDownAfterMilliThroughProxy();

    int getSentinelRateLimitSize();

    boolean isSentinelRateLimitOpen();

    QuorumConfig getDefaultSentinelQuorumConfig();

    Set<DcClusterDelayMarkDown> getDelayedMarkDownDcClusters();

    int getStableLossAfterRounds();

    int getStableRecoverAfterRounds();

    int getStableResetAfterRounds();

    float getSiteStableThreshold();

    float getSiteUnstableThreshold();

    Boolean getSiteStable();

    int getQuorum();

    Set<String> getIgnoredHealthCheckDc();

    int getClustersPartIndex();

    int getCheckerReportIntervalMilli();

    int getCheckerMetaRefreshIntervalMilli();

    String getConsoleAddress();

    int getCheckerAckIntervalMilli();

    Set<String> getAllCheckerAddress();

    long getConfigCacheTimeoutMilli();

    int getProxyCheckUpRetryTimes();

    int getProxyCheckDownRetryTimes();

    boolean supportSentinelHealthCheck(ClusterType clusterType, String clusterName);

    void register(List<String> keys, ConfigChangeListener configListener);

    String sentinelCheckDowngradeStrategy();

    String crossDcSentinelMonitorNameSuffix();

    int getNonCoreCheckIntervalMilli();

    Set<String> getOuterClusterTypes();

    Map<String, String> sentinelMasterConfig();

    long subscribeTimeoutMilli();

    String getDcsRelations();

    int maxRemovedDcsCnt();

    int maxRemovedClustersPercent();

    int getKeeperCheckerIntervalMilli();

}
