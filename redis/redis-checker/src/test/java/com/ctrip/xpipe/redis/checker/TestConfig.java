package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.redis.checker.alert.AlertConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DcClusterDelayMarkDown;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;

import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/3/14
 */
public class TestConfig implements CheckerConfig, AlertConfig {

    @Override
    public String getXpipeRuntimeEnvironment() {
        return null;
    }

    @Override
    public Set<String> getAlertWhileList() {
        return null;
    }

    @Override
    public int getNoAlarmMinutesForClusterUpdate() {
        return 0;
    }

    @Override
    public String getXPipeAdminEmails() {
        return "XPipeAdmin@email.com";
    }

    @Override
    public Map<String, String> getConsoleDomains() {
        return null;
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
        return null;
    }

    @Override
    public String getConsoleDomain() {
        return null;
    }

    @Override
    public int getRedisReplicationHealthCheckInterval() {
        return 0;
    }

    @Override
    public int getClusterHealthCheckInterval() {
        return 0;
    }

    @Override
    public int getDownAfterCheckNums() {
        return 0;
    }

    @Override
    public int getDownAfterCheckNumsThroughProxy() {
        return 0;
    }

    @Override
    public int getRedisConfCheckIntervalMilli() {
        return 300000;
    }

    @Override
    public int getSentinelCheckIntervalMilli() {
        return 0;
    }

    @Override
    public int getHealthyDelayMilli() {
        return 0;
    }

    @Override
    public int getHealthyDelayMilliThroughProxy() {
        return 0;
    }

    @Override
    public String getReplDisklessMinRedisVersion() {
        return null;
    }

    @Override
    public String getXRedisMinimumRequestVersion() {
        return null;
    }

    @Override
    public int getPingDownAfterMilli() {
        return 0;
    }

    @Override
    public int getPingDownAfterMilliThroughProxy() {
        return 0;
    }

    @Override
    public int getSentinelRateLimitSize() {
        return 0;
    }

    @Override
    public boolean isSentinelRateLimitOpen() {
        return false;
    }

    @Override
    public QuorumConfig getDefaultSentinelQuorumConfig() {
        return null;
    }

    @Override
    public Set<DcClusterDelayMarkDown> getDelayedMarkDownDcClusters() {
        return null;
    }

    @Override
    public boolean isConsoleSiteUnstable() {
        return false;
    }

    @Override
    public int getQuorum() {
        return 0;
    }

    @Override
    public Set<String> getIgnoredHealthCheckDc() {
        return null;
    }
}
