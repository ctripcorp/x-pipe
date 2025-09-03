package com.ctrip.xpipe.redis.console.config;

import com.ctrip.xpipe.redis.checker.alert.AlertConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.console.config.model.BeaconOrgRoute;
import com.ctrip.xpipe.redis.console.util.HickwallMetricInfo;
import com.ctrip.xpipe.redis.core.config.CoreConfig;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author shyin
 *
 * Oct 19, 2016
 */
public interface ConsoleConfig extends CoreConfig, CheckerConfig, AlertConfig {

	String getServerMode();

	String getDatasource();

	int getConsoleNotifyRetryTimes();

	int getConsoleNotifyRetryInterval();

	Map<String,String> getMetaservers();

	int getConsoleNotifyThreads();

	Set<String> getConsoleUserAccessWhiteList();

	int getRedisReplicationHealthCheckInterval();

	int getClusterHealthCheckInterval();

	Map<String,String> getHickwallClusterMetricFormat();

	HickwallMetricInfo getHickwallMetricInfo();

	int getHealthyDelayMilli();

	int getHealthyDelayMilliThroughProxy();

	int getDownAfterCheckNums();

	int getDownAfterCheckNumsThroughProxy();

	int getCacheRefreshInterval();

	Set<String> getAlertWhileList();

	int getQuorum();

	int getIsolateAfterRounds();

	int getIsolateRecoverAfterRounds();

	Boolean getDcIsolated();

	String delegateDc();

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

	int getHealthCheckSuspendMinutes();

	Set<String> getIgnoredHealthCheckDc();

	int getPingDownAfterMilli();

	int getPingDownAfterMilliThroughProxy();

	Pair<String, String> getClusterShardForMigrationSysCheck();

	int getProxyInfoCollectInterval();

	int getOutterClientCheckInterval();

	int getOuterClientSyncInterval();

	String getOuterClientToken();

	Map<String, String> getConsoleDomains();

	boolean isSentinelRateLimitOpen();

	int getSentinelRateLimitSize();

	Set<String> getVariablesCheckDataSources();

	Set<String> getOwnClusterType();

	Set<String> shouldNotifyClusterTypes();

	String getCrossDcLeaderLeaseName();

	List<BeaconOrgRoute> getBeaconOrgRoutes();

	int getClusterDividedParts();

	int getCheckerAckTimeoutMilli();

	long getMigrationTimeoutMilli();

	long getServletMethodTimeoutMilli();

	boolean isRedisConfigCheckMonitorOpen();

	String getRedisConfigCheckRules();

	String getChooseRouteStrategyType();

	boolean isAutoMigrateOverloadKeeperContainerOpen();

	long getAutoMigrateOverloadKeeperContainerIntervalMilli();

	double getKeeperPairOverLoadFactor();

	double getKeeperContainerDiskOverLoadFactor();

	double getKeeperContainerIoRate();

	long getMetaServerSlotClusterMapCacheTimeOutMilli();

	boolean autoSetKeeperSyncLimit();

	boolean disableDb();

	Set<String> getExtraSyncDC();

	String getConsoleNoDbDomain();

	String getHttpAcceptEncoding();

	int getCRedisClusterCacheRefreshIntervalMilli();

	long getKeeperContainerDiskInfoCollectIntervalMilli();

	int getDcMetaBuildConcurrent();

	long getCheckIsolateInterval();
}
