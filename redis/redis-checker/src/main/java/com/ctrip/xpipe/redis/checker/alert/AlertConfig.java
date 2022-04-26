package com.ctrip.xpipe.redis.checker.alert;

import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/3/9
 */
public interface AlertConfig {

    String KEY_XPIPE_RUNTIME_ENVIRONMENT = "xpipe.runtime.environment";

    String KEY_ALERT_WHITE_LIST = "console.alert.whitelist";

    String KEY_ALERT_CLUSTER_EXCLUDED_REGEX = "console.alert.excluded.regex";

    String KEY_NO_ALARM_MUNITE_FOR_CLUSTER_UPDATE = "no.alarm.minute.for.cluster.update";

    String KEY_XPIPE_ADMIN_EMAILS = "xpipe.admin.emails";

    String KEY_CONSOLE_DOMAINS = "console.domains";

    String KEY_ALERT_MESSAGE_SUSPEND_TIME = "alert.message.suspend.time";

    String KEY_DBA_EMAILS = "redis.dba.emails";

    String KEY_ALERT_MESSAGE_RECOVER_TIME = "alert.message.recover.time";

    String KEY_REDIS_ALERT_SENDER_EMAIL = "redis.alert.sender.email";

    String KEY_DOMAIN = "console.domain";

    String getXpipeRuntimeEnvironment();

    Set<String> getAlertWhileList();

    int getNoAlarmMinutesForClusterUpdate();

    String getXPipeAdminEmails();

    Map<String, String> getConsoleDomains();

    int getAlertSystemSuspendMinute();

    String getDBAEmails();

    int getAlertSystemRecoverMinute();

    String getRedisAlertSenderEmail();

    String getConsoleDomain();

    String getClusterExcludedRegex();

}
