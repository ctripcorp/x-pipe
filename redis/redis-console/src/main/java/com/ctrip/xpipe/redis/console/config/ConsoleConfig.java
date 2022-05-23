package com.ctrip.xpipe.redis.console.config;

import com.ctrip.xpipe.redis.checker.alert.AlertConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DcClusterDelayMarkDown;
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
public interface ConsoleConfig extends CoreConfig, CheckerConfig, AlertConfig {
	
	String getDatasource();
	
	int getConsoleNotifyRetryTimes();
	
	int getConsoleNotifyRetryInterval();

	Map<String,String> getMetaservers();
	
	int getConsoleNotifyThreads();

	Set<String> getConsoleUserAccessWhiteList();
	
	int getRedisReplicationHealthCheckInterval();

	int getClusterHealthCheckInterval();

	String getHickwallClusterMetricFormat();

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

	Map<String, String> getSocketStatsAnalyzingKeys();

	Pair<String, String> getClusterShardForMigrationSysCheck();

	int getProxyInfoCollectInterval();

	int getOutterClientCheckInterval();

	int getOuterClientSyncInterval();

	String filterOuterClusters();

	Map<String, String> getConsoleDomains();

	boolean isSentinelRateLimitOpen();

	int getSentinelRateLimitSize();

	Set<String> getVariablesCheckDataSources();

	Set<String> getOwnClusterType();

	Set<String> shouldNotifyClusterTypes();

	String getCrossDcLeaderLeaseName();

	boolean isSensitiveForRedundantRedis();

	String getParallelConsoleDomain();

	boolean isConsoleSiteUnstable();

	String getDefaultBeaconHost();

	Map<Long, String> getBeaconHosts();

	int getClusterDividedParts();

	int getCheckerAckTimeoutMilli();

	long getMigrationTimeoutMilli();

	long getServletMethodTimeoutMilli();
	
	boolean isRedisConfigCheckMonitorOpen();

	String getRedisConfigCheckRules();

	Set<String> getClustersSupportBiMigration();

	String getBeaconSupportZone();

	String getBiDirectionMigrationDcPriority();

	String getChooseRouteStrategyType();

}
