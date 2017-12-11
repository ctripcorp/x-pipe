package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author shyin
 *         <p>
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
    public static final String KEY_REDIS_CONF_CHECK_INTERVAL = "redis.conf.check.interval";
    public static final String KEY_HICKWALL_ADDRESS = "console.hickwall.address";
    public static final String KEY_HEALTHY_DELAY = "console.healthy.delay";
    public static final String KEY_DOWN_AFTER_CHECK_NUMS = "console.down.after.checknums";
    public static final String KEY_CACHE_REFERSH_INTERVAL = "console.cache.refresh.interval";
    public static final String KEY_ALERT_WHITE_LIST = "console.alert.whitelist";
    public static final String KEY_ALL_CONSOLES = "console.all.addresses";
    public static final String KEY_QUORUM = "console.quorum";
    public static final String KEY_DOMAIN = "console.domain";
    public static final String KEY_CNAME_TODC = "console.cname.todc";

    public static final String KEY_SENTINEL_QUORUM = "console.sentinel.quorum";

    private static final String KEY_REDIS_REPL_DISKLESS_MINIMUM_VERSION = "redis.repl.diskless.minimum.version";
    private static final String KEY_XREDIS_REQUEST_MINI_VERSION = "xredis.minimum.request.version";

    private static final String KEY_DBA_EMAILS = "redis.dba.emails";
    private static final String KEY_REDIS_ALERT_SENDER_EMAIL = "redis.alert.sender.email";
    private static final String KEY_XPIPE_RUNTIME_ENVIRONMENT = "xpipe.runtime.environment";
    private static final String KEY_XPIPE_ADMIN_EMAILS = "xpipe.admin.emails";

    private static final String KEY_ALERT_MESSAGE_SUSPEND_TIME = "alert.message.suspend.time";

    private static final String KEY_ALERT_MESSAGE_RECOVER_TIME = "alert.message.recover.time";

    private static final String KEY_CONFIG_DEFAULT_RESTORE_HOUR = "console.config.default.restore.hour";

    @Override
    public int getAlertSystemRecoverMinute() {
        return getIntProperty(KEY_ALERT_MESSAGE_RECOVER_TIME, 5);
    }

    @Override
    public int getAlertSystemSuspendMinute() {
        return getIntProperty(KEY_ALERT_MESSAGE_SUSPEND_TIME, 30);
    }

    @Override
    public String getDBAEmails() {
        return getProperty(KEY_DBA_EMAILS, "DBA@email.com");
    }

    @Override
    public String getRedisAlertSenderEmail() {
        return getProperty(KEY_REDIS_ALERT_SENDER_EMAIL, "");
    }

    @Override
    public String getXpipeRuntimeEnvironmentEnvironment() {
        return getProperty(KEY_XPIPE_RUNTIME_ENVIRONMENT, "");
    }

    @Override
    public String getXPipeAdminEmails() {
        return getProperty(KEY_XPIPE_ADMIN_EMAILS, "XPipeAdmin@email.com");
    }

    @Override
    public String getReplDisklessMinRedisVersion() {
        return getProperty(KEY_REDIS_REPL_DISKLESS_MINIMUM_VERSION, "2.8.22");
    }

    @Override
    public String getXRedisMinimumRequestVersion() {
        return getProperty(KEY_XREDIS_REQUEST_MINI_VERSION, "0.0.1");
    }

    @Override
    public String getDatasource() {
        return getProperty(KEY_DATASOURCE, "fxxpipe");
    }

    @Override
    public int getConsoleNotifyRetryTimes() {
        return getIntProperty(KEY_CONSOLE_NOTIFY_RETRY_TIMES, 3);
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
        return getProperty(KEY_HICKWALL_ADDRESS, "");
    }

    @Override
    public int getHealthyDelayMilli() {
        return getIntProperty(KEY_HEALTHY_DELAY, 2000);
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
    public Set<String> getAlertWhileList() {

        HashSet result = new HashSet();
        String whitelist = getProperty(KEY_ALERT_WHITE_LIST, "");
        String[] split = whitelist.split("\\s*(,|;)\\s*");

        for(String sp : split){
            if(!StringUtil.isEmpty(sp)){
                result.add(sp);
            }
        }

        return result;

    }

    @Override
    public String getAllConsoles() {
        return getProperty(KEY_ALL_CONSOLES, "127.0.0.1:8080");
    }

    @Override
    public int getQuorum() {
        return getIntProperty(KEY_QUORUM, 1);
    }

    @Override
    public int getRedisConfCheckIntervalMilli() {

        return getIntProperty(
                KEY_REDIS_CONF_CHECK_INTERVAL,
                Integer.parseInt(System.getProperty(KEY_REDIS_CONF_CHECK_INTERVAL, "300000"))
        );
    }

    @Override
    public String getConsoleDomain() {
        return getProperty(KEY_DOMAIN, "127.0.0.1");
    }

    @Override
    public Map<String, String> getConsoleCnameToDc() {

        String property = getProperty(KEY_CNAME_TODC, "{}");
        return JsonCodec.INSTANCE.decode(property, Map.class);
    }

    @Override
    public QuorumConfig getDefaultSentinelQuorumConfig() {

        String config = getProperty(KEY_SENTINEL_QUORUM, "{}");
        return JsonCodec.INSTANCE.decode(config, QuorumConfig.class);
    }

    @Override
    public int getConfigDefaultRestoreHours() {
        return getIntProperty(KEY_CONFIG_DEFAULT_RESTORE_HOUR, 10);
    }
}
