package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.redis.checker.alert.AlertConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DcClusterDelayMarkDown;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
    public int getHealthyDelayMilliThroughProxy() {
        return 30 * 1000;
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
}
