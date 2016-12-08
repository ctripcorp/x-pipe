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
	public static final String KEY_HICKWALL_HOST_PORT = "hickwall.host.port";
	public static final String KEY_HICKWALL_QUEUE_SIZE = "hickwall.queue.size";
	public static final String KEY_REDIS_REPLICATION_HEALTH_CHECK_INTERVAL = "redis.replication.health.check.interval";

	@Override
	public String getDatasource() {
		return getProperty(KEY_DATASOURCE);
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
	public String getHickwallHostPort() {
		return getProperty(KEY_HICKWALL_HOST_PORT, "");
	}

	@Override
	public int getHickwallQueueSize() {
		return getIntProperty(KEY_HICKWALL_QUEUE_SIZE, 100 * 1000);
	}

	@Override
	public int getRedisReplicationHealthCheckInterval() {
		return getIntProperty(KEY_REDIS_REPLICATION_HEALTH_CHECK_INTERVAL, 10000);
	}

}
