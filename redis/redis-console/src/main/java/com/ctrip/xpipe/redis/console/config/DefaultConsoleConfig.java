package com.ctrip.xpipe.redis.console.config;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;

/**
 * @author shyin
 *
 *         Oct 19, 2016
 */
public class DefaultConsoleConfig extends AbstractCoreConfig implements ConsoleConfig {

	public static final String KEY_DATASOURCE = "datasource";
	public static final String KEY_CONSOLE_NOTIFY_RETRY_TIMES = "console.notify.retry.times";
	public static final String KEY_CONSOLE_NOTIFY_THREADS = "console.notify.threads";
	public static final String KEY_CONSOLE_NOTIFY_RETRY_INTERVAL = "console.notify.retry.interval";
	public static final String KEY_METASERVERS = "metaservers";
	public static final String KEY_USER_ACCESS_WHITE_LIST = "user.access.white.list";
	public static final String KEY_REDIS_REPLICATION_HEALTH_CHECK_INTERVAL = "redis.replication.health.check.interval";
	public static final String KEY_HICKWALL_ADDRESS = "console.hickwall.address";
	public static final String KEY_HEALTHY_DELAY = "console.healthy.delay";
	public static final String KEY_DOWN_AFTER_CHECK_NUMS = "console.down.after.checknums";
	public static final String KEY_CACHE_REFERSH_INTERVAL = "console.cache.refresh.interval";
	public static final String KEY_ALERT_WHITE_LIST = "console.alert.whitelist";



	@Override
	public String getDatasource() {
		return getProperty(KEY_DATASOURCE, "fxxpipe");
	}

	@Override
	public int getConsoleNotifyRetryTimes() {
		return getIntProperty(KEY_CONSOLE_NOTIFY_RETRY_TIMES, 10);
	}

	@Override
	public int getConsoleNotifyRetryInterval() {
		return getIntProperty(KEY_CONSOLE_NOTIFY_RETRY_INTERVAL, 100);
	}

	@Override
	public String getMetaservers() {
		return getProperty(KEY_METASERVERS, "{}");
	}

	@Override
	public int getConsoleNotifyThreads() {
		return getIntProperty(KEY_CONSOLE_NOTIFY_THREADS, 20);
	}

	@Override
	public Set<String> getConsoleUserAccessWhiteList() {
		String whiteList = getProperty(KEY_USER_ACCESS_WHITE_LIST, "*");
		return new HashSet<>(Arrays.asList(whiteList.split(",")));
	}
	
	@Override
	public int getRedisReplicationHealthCheckInterval() {
		return getIntProperty(KEY_REDIS_REPLICATION_HEALTH_CHECK_INTERVAL, 2000);
	}
	
	@Override
	public String getHickwallAddress() {
		return getProperty(KEY_HICKWALL_ADDRESS,"");
	}

	@Override
	public int getHealthyDelayMilli() {
		return getIntProperty(KEY_HEALTHY_DELAY, 5000);
	}

	@Override
	public int getDownAfterCheckNums() {
		return getIntProperty(KEY_DOWN_AFTER_CHECK_NUMS, 5);
	}

	@Override
	public int getCacheRefreshInterval() {
		return getIntProperty(KEY_CACHE_REFERSH_INTERVAL, 1000);
	}

	@Override
	public String getAlertWhileList() {
		return getProperty(KEY_ALERT_WHITE_LIST, "");
	}

}
