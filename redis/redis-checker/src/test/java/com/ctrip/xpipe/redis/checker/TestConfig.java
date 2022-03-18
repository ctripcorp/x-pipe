package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.api.config.ConfigChangeListener;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.alert.AlertConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DcClusterDelayMarkDown;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;

import java.util.*;

/**
 * @author lishanglin
 * date 2021/3/14
 */
public class TestConfig implements CheckerConfig, AlertConfig {

    @Override
    public String getXpipeRuntimeEnvironment() {
        return "";
    }

    @Override
    public Set<String> getAlertWhileList() {
        return new HashSet<>();
    }

    @Override
    public int getNoAlarmMinutesForClusterUpdate() {
        return 15;
    }

    @Override
    public String getXPipeAdminEmails() {
        return "XPipeAdmin@email.com";
    }

    @Override
    public Map<String, String> getConsoleDomains() {
        return new HashMap<>();
    }

    @Override
    public int getAlertSystemSuspendMinute() {
        return 30;
    }

    @Override
    public String getDBAEmails() {
        return "DBA@email.com";
    }

    @Override
    public int getAlertSystemRecoverMinute() {
        return 5;
    }

    @Override
    public String getRedisAlertSenderEmail() {
        return "";
    }

    @Override
    public String getConsoleDomain() {
        return "127.0.0.1";
    }

    @Override
    public int getRedisReplicationHealthCheckInterval() {
        return 2000;
    }

    @Override
    public int getClusterHealthCheckInterval() {
        return 300000;
    }

    @Override
    public int getDownAfterCheckNums() {
        return 5;
    }

    @Override
    public int getDownAfterCheckNumsThroughProxy() {
        return 10;
    }

    @Override
    public int getRedisConfCheckIntervalMilli() {
        return 300000;
    }

    @Override
    public int getSentinelCheckIntervalMilli() {
        return 300000;
    }

    @Override
    public int getHealthyDelayMilli() {
        return 2000;
    }

    @Override
    public int getHealthyLeastNotifyIntervalMilli() {
        return 60 * 1000;
    }

    @Override
    public int getHealthyDelayMilliThroughProxy() {
        return 30 * 1000;
    }

    @Override
    public int getInstanceLongDelayMilli() {
        return 3 * 60 * 1000;
    }

    @Override
    public String getReplDisklessMinRedisVersion() {
        return "2.8.22";
    }

    @Override
    public String getXRedisMinimumRequestVersion() {
        return "0.0.3";
    }

    @Override
    public int getPingDownAfterMilli() {
        return 12000;
    }

    @Override
    public int getPingDownAfterMilliThroughProxy() {
        return 30000;
    }

    @Override
    public int getSentinelRateLimitSize() {
        return 3;
    }

    @Override
    public boolean isSentinelRateLimitOpen() {
        return false;
    }

    @Override
    public QuorumConfig getDefaultSentinelQuorumConfig() {
        return new QuorumConfig();
    }

    @Override
    public Set<DcClusterDelayMarkDown> getDelayedMarkDownDcClusters() {
        return new HashSet<>();
    }

    @Override
    public boolean isConsoleSiteUnstable() {
        return true;
    }

    @Override
    public int getQuorum() {
        return 1;
    }

    @Override
    public Set<String> getIgnoredHealthCheckDc() {
        return new HashSet<>();
    }

    @Override
    public int getClustersPartIndex() {
        return 0;
    }

    @Override
    public int getCheckerReportIntervalMilli() {
        return 10000;
    }

    @Override
    public int getCheckerMetaRefreshIntervalMilli() {
        return 30000;
    }

    @Override
    public String getConsoleAddress() {
        return "http://localhost:8080";
    }

    @Override
    public int getCheckerAckIntervalMilli() {
        return 10000;
    }

    @Override
    public Set<String> getAllCheckerAddress() {
        return Collections.singleton("127.0.0.1:8080");
    }

    @Override
    public long getConfigCacheTimeoutMilli() {
        return 0L;
    }

    @Override
    public boolean supportSentinelHealthCheck(ClusterType clusterType, String clusterName) {
        return clusterType.supportHealthCheck();
    }

    @Override
    public String sentinelCheckDowngradeStrategy() {
        return "lessThanHalf";
    }

    @Override
    public int getProxyCheckUpRetryTimes() {
        return 10;
    }

    @Override
    public int getProxyCheckDownRetryTimes() {
        return 1;
    }

    @Override
    public void register(List<String> keys, ConfigChangeListener configListener) {

    }

    @Override
    public String crossDcSentinelMonitorNameSuffix() {
        return null;
    }

    @Override
    public int getNonCoreCheckIntervalMilli() {
        return 3 * 60 * 60 *1000;
    }
  
    @Override
    public boolean shouldBindOuterClusterShardAndSentinel() {
        return false;
    }

    @Override
    public int sentinelBindTimeoutMilli() {
        return 0;
    }

    @Override
    public Set<String> getOuterClusterTypes() {
        return null;
    }

    @Override
    public Map<String, String> sentinelMasterConfig() {
        return null;
    }

    @Override
    public long subscribeTimeoutMilli() {
        return 5000L;
    }
}
