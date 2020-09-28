package com.ctrip.xpipe.redis.console.config;

import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.DcClusterDelayMarkDown;
import com.ctrip.xpipe.redis.console.util.HickwallMetricInfo;
import com.ctrip.xpipe.redis.core.config.CoreConfig;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.tuple.Pair;

import java.util.Map;
import java.util.Set;

/**
 * @author shyin
 *
 * Oct 19, 2016
 */
public interface ConsoleConfig extends CoreConfig {
	
	String getDatasource();
	
	int getConsoleNotifyRetryTimes();
	
	int getConsoleNotifyRetryInterval();
	
	String getMetaservers();
	
	int getConsoleNotifyThreads();

	Set<String> getConsoleUserAccessWhiteList();
	
	int getRedisReplicationHealthCheckInterval();

	HickwallMetricInfo getHickwallMetricInfo();

	int getHealthyDelayMilli();

	int getHealthyDelayMilliThroughProxy();

	int getDownAfterCheckNums();

	int getDownAfterCheckNumsThroughProxy();

	int getCacheRefreshInterval();

	Set<String> getAlertWhileList();

	String getAllConsoles();

	int getQuorum();

	int getRedisConfCheckIntervalMilli();

	int getSentinelCheckIntervalMilli();

	String getConsoleDomain();

	QuorumConfig  getDefaultSentinelQuorumConfig();

	String getReplDisklessMinRedisVersion();

	String getXRedisMinimumRequestVersion();

	String getXpipeRuntimeEnvironment();

	String getDBAEmails();

	String getRedisAlertSenderEmail();

	String getXPipeAdminEmails();

	int getAlertSystemSuspendMinute();

	int getAlertSystemRecoverMinute();

	int getConfigDefaultRestoreHours();

	int getRebalanceSentinelInterval();

	int getRebalanceSentinelMaxNumOnce();

	int getNoAlarmMinutesForClusterUpdate();

	Set<String> getIgnoredHealthCheckDc();

	Set<DcClusterDelayMarkDown> getDelayedMarkDownDcClusters();

	int getPingDownAfterMilli();

	int getPingDownAfterMilliThroughProxy();

	void register(ConsoleConfigListener listener);

	Map<String, String> getSocketStatsAnalyzingKeys();

	Pair<String, String> getClusterShardForMigrationSysCheck();

	int getProxyInfoCollectInterval();

	int getOutterClientCheckInterval();

	Map<String, String> getConsoleDomains();

	boolean isSentinelRateLimitOpen();

	int getSentinelRateLimitSize();

	Set<String> getVariablesCheckDataSources();

	Set<String> getOwnClusterType();

	String getCrossDcLeaderLeaseName();

	boolean isSensitiveForRedundantRedis();

	String getParallelConsoleDomain();

	boolean isConsoleSiteUnstable();
}
