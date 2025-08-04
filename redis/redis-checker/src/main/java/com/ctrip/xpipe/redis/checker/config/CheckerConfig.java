package com.ctrip.xpipe.redis.checker.config;

import com.ctrip.xpipe.api.config.ConfigChangeListener;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/3/8
 */
public interface CheckerConfig {

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

    int getMarkInstanceBaseDelayMilli();

    int getMarkdownInstanceMaxDelayMilli();

    int getMarkupInstanceMaxDelayMilli();

    boolean getShouldDoAfterNettyClientConnected();

}
