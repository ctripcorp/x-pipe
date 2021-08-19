package com.ctrip.xpipe.redis.checker.config;

import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DcClusterDelayMarkDown;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;

import java.util.Set;

/**
 * @author lishanglin
 * date 2021/3/8
 */
public interface CheckerConfig {

    String KEY_CLUSTER_HEALTH_CHECK_INTERVAL = "cluster.health.check.interval";

    String KEY_REDIS_REPLICATION_HEALTH_CHECK_INTERVAL = "redis.replication.health.check.interval";

    String KEY_HEALTHY_DELAY = "console.healthy.delay";

    String KEY_REDIS_CONF_CHECK_INTERVAL = "redis.conf.check.interval";

    String KEY_DOWN_AFTER_CHECK_NUMS = "console.down.after.checknums";

    String KEY_REDIS_REPL_DISKLESS_MINIMUM_VERSION = "redis.repl.diskless.minimum.version";

    String KEY_XREDIS_REQUEST_MINI_VERSION = "xredis.minimum.request.version";

    String KEY_HEALTHY_DELAY_THROUGH_PROXY = "console.healthy.delay.through.proxy";

    String KEY_DOWN_AFTER_CHECK_NUMS_THROUGH_PROXY = "console.down.after.checknums.through.proxy";

    String KEY_PING_DOWN_AFTER_MILLI = "console.ping.down.after.milli";

    String KEY_PING_DOWN_AFTER_MILLI_THROUGH_PROXY = "console.ping.down.after.milli.through.proxy";

    String KEY_SENTINEL_CHECK_INTERVAL = "console.health.sentinel.interval";

    String KEY_SENTINEL_CHECK_CLUSTER_TYPE = "console.health.sentinel.check.type";

    String KEY_SENTINEL_CHECK_CLUSTERS = "console.health.sentinel.check.clusters";

    String KEY_SENTINEL_RATE_LIMIT_SIZE = "console.sentinel.rate.limit.size";

    String KEY_SENTINEL_RATE_LIMIT_OPEN = "console.sentinel.rate.limit.open";

    String KEY_SENTINEL_QUORUM = "console.sentinel.quorum";

    String KEY_DC_CLUSTER_WONT_MARK_DOWN = "console.dc.cluster.pairs.delay.mark.down";

    String KEY_DEFAULT_MARK_DOWN_DELAY_SEC = "console.default.mark.down.delay.sec";

    String KEY_CONSOLE_SITE_STABLE = "console.site.stable";

    String KEY_QUORUM = "console.quorum";

    String KEY_IGNORED_DC_FOR_HEALTH_CHECK = "ignored.dc.for.health.check";

    String KEY_CLUSTERS_PART_INDEX = "checker.clusters.part.index";

    String KEY_CHECKER_REPORT_INTERVAL = "checker.report.interval.milli";

    String KEY_CHECKER_META_REFRESH_INTERVAL = "checker.meta.refresh.interval.milli";

    String KEY_CONSOLE_ADDRESS = "console.address";

    String KEY_CHECKER_ACK_INTERVAL = "checker.ack.interval.milli";

    String KEY_CHECKER_ADDRESS_ALL = "checker.address.all";

    String KEY_CONFIG_CACHE_TIMEOUT_MILLI = "checker.config.cache.timeout.milli";

    int getRedisReplicationHealthCheckInterval();

    int getClusterHealthCheckInterval();

    int getDownAfterCheckNums();

    int getDownAfterCheckNumsThroughProxy();

    int getRedisConfCheckIntervalMilli();

    int getSentinelCheckIntervalMilli();

    boolean checkClusterType();

    Set<String> commonClustersSupportSentinelCheck();

    int getHealthyDelayMilli();

    int getHealthyDelayMilliThroughProxy();

    String getReplDisklessMinRedisVersion();

    String getXRedisMinimumRequestVersion();

    int getPingDownAfterMilli();

    int getPingDownAfterMilliThroughProxy();

    int getSentinelRateLimitSize();

    boolean isSentinelRateLimitOpen();

    QuorumConfig getDefaultSentinelQuorumConfig();

    Set<DcClusterDelayMarkDown> getDelayedMarkDownDcClusters();

    boolean isConsoleSiteUnstable();

    int getQuorum();

    Set<String> getIgnoredHealthCheckDc();

    int getClustersPartIndex();

    int getCheckerReportIntervalMilli();

    int getCheckerMetaRefreshIntervalMilli();

    String getConsoleAddress();

    int getCheckerAckIntervalMilli();

    Set<String> getAllCheckerAddress();

    long getConfigCacheTimeoutMilli();

}
