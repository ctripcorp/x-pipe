package com.ctrip.xpipe.redis.console.config;

import com.ctrip.xpipe.redis.core.config.CoreConfig;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;

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
	
	String getHickwallAddress();

	int getHealthyDelayMilli();

	int getDownAfterCheckNums();

	int getCacheRefreshInterval();

	Set<String> getAlertWhileList();

	String getAllConsoles();

	int getQuorum();

	int getRedisConfCheckIntervalMilli();

	String getConsoleDomain();

	Map<String, String> getConsoleCnameToDc();

	QuorumConfig  getDefaultSentinelQuorumConfig();

	String getReplDisklessMinRedisVersion();

	String getXRedisMinimumRequestVersion();

	String getXpipeRuntimeEnvironmentEnvironment();

	String getDBAEmails();

	String getRedisAlertSenderEmail();

	String getXPipeAdminEmails();

	int getAlertSystemSuspendMinute();

	int getAlertSystemRecoverMinute();

	int getConfigDefaultRestoreHours();

	int getRebalanceSentinelInterval();

	int getRebalanceSentinelMaxNumOnce();

	int getNoAlarmMinutesForNewCluster();
}
