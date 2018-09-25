package com.ctrip.xpipe.redis.console.alert;

import static com.ctrip.xpipe.redis.console.alert.policy.receiver.EmailReceiver.EMAIL_DBA;
import static com.ctrip.xpipe.redis.console.alert.policy.receiver.EmailReceiver.EMAIL_XPIPE_ADMIN;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 16, 2017
 */
public enum ALERT_TYPE {

    CLIENT_INSTANCE_NOT_OK("client_status", EMAIL_DBA | EMAIL_XPIPE_ADMIN, true),
    QUORUM_DOWN_FAIL("quorum_fail", EMAIL_XPIPE_ADMIN, false),
    SENTINEL_RESET("stl_rst", EMAIL_XPIPE_ADMIN, false),
    REDIS_CONF_REWRITE_FAILURE("redis_conf_rewrite_failure", EMAIL_DBA|EMAIL_XPIPE_ADMIN, false),
    CLIENT_INCONSIS("client_inconsis", EMAIL_DBA | EMAIL_XPIPE_ADMIN, true),
    MIGRATION_MANY_UNFINISHED("migra_unfinish", EMAIL_XPIPE_ADMIN, true),
    XREDIS_VERSION_NOT_VALID("xredis_version_not_valid", EMAIL_DBA, true),
    REDIS_REPL_DISKLESS_SYNC_ERROR("redis_repl_diskless_sync_error", EMAIL_DBA, true),
    MARK_INSTANCE_UP("mark instance up", EMAIL_DBA, false),
    MARK_INSTANCE_DOWN("mark instance down", EMAIL_DBA, false),
    ALERT_SYSTEM_OFF("alert system is turning off", EMAIL_DBA | EMAIL_XPIPE_ADMIN, true, false),
    SENTINEL_AUTO_PROCESS_OFF("sentinel auto process is turning off", EMAIL_DBA | EMAIL_XPIPE_ADMIN, true, false),
    REPL_BACKLOG_NOT_ACTIVE("repl_backlog_not_active", EMAIL_DBA | EMAIL_XPIPE_ADMIN, true),
    SENTINEL_MONITOR_REDUNDANT_REDIS("sentinel_monitors_redundant_redis", EMAIL_XPIPE_ADMIN, false),
    SENTINEL_MONITOR_INCONSIS("sentinel_monitor_incosis", EMAIL_XPIPE_ADMIN, false),
    INSTANCE_SICK_BUT_DELAY_MARK_DOWN("instance_lag_delay_mark_down", EMAIL_XPIPE_ADMIN | EMAIL_DBA, false)
    ;

    private String simpleDesc;

    private int alertMethod;

    private boolean urgent;

    private boolean reportRecovery;

    ALERT_TYPE(String simpleDesc, int alertMethod, boolean reportRecovery){
        this(simpleDesc, alertMethod, false, reportRecovery);
    }

    ALERT_TYPE(String simpleDesc, int alertMethod, boolean urgent, boolean reportRecovery){
        this.simpleDesc = simpleDesc;
        this.alertMethod = alertMethod;
        this.urgent = urgent;
        this.reportRecovery = reportRecovery;
    }

    public String simpleDesc() {
        return simpleDesc;
    }

    public int getAlertMethod() {
        return alertMethod;
    }

    public boolean urgent() {
        return this.urgent;
    }

    public boolean reportRecovery() {
        return reportRecovery;
    }
}
