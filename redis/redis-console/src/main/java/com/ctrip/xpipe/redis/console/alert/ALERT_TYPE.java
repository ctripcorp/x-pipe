package com.ctrip.xpipe.redis.console.alert;

import static com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManager.EMAIL_DBA;
import static com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManager.EMAIL_XPIPE_ADMIN;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 16, 2017
 */
public enum ALERT_TYPE {

    CLIENT_INSTANCE_NOT_OK("client_status", EMAIL_DBA | EMAIL_XPIPE_ADMIN, 5),
    QUORUM_DOWN_FAIL("quorum_fail", EMAIL_XPIPE_ADMIN, 5),
    SENTINEL_RESET("stl_rst", EMAIL_DBA | EMAIL_XPIPE_ADMIN, 5),
    REDIS_CONF_REWRITE_FAILURE("redis_conf_rewrite_failure", EMAIL_DBA|EMAIL_XPIPE_ADMIN, 5),
    CLIENT_INCONSIS("client_inconsis", EMAIL_DBA | EMAIL_XPIPE_ADMIN, 5),
    MIGRATION_MANY_UNFINISHED("migra_unfinish", EMAIL_XPIPE_ADMIN, 5),
    XREDIS_VERSION_NOT_VALID("xredis_version_not_valid", EMAIL_DBA, 5),
    REDIS_REPL_DISKLESS_SYNC_ERROR("redis_repl_diskless_sync_error", EMAIL_DBA, 5),
    MARK_INSTANCE_UP("mark instance up", EMAIL_DBA, 5),
    MARK_INSTANCE_DOWN("mark instance down", EMAIL_DBA, 5),
    ALERT_SYSTEM_OFF("alert system is turning off", EMAIL_DBA | EMAIL_XPIPE_ADMIN, 0),
    SENTINEL_AUTO_PROCESS_OFF("sentinel auto process is turning off", EMAIL_DBA | EMAIL_XPIPE_ADMIN, 0),
    REPL_BACKLOG_NOT_ACTIVE("repl_backlog_not_active", EMAIL_DBA | EMAIL_XPIPE_ADMIN, 5),
    SENTINEL_MONITOR_REDUNDANT_REDIS("sentinel_monitors_redundant_redis", EMAIL_XPIPE_ADMIN, 0),
    SENTINEL_MONITOR_INCONSIS("sentinel_monitor_incosis", EMAIL_XPIPE_ADMIN, 0)
    ;

    private String simpleDesc;

    private int alertPolicy;

    private int recoverTime;

    ALERT_TYPE(String simpleDesc, int alertPolicyId, int recoverTime){
        this.simpleDesc = simpleDesc;
        this.alertPolicy = alertPolicyId;
        this.recoverTime = recoverTime;
    }

    public String simpleDesc() {
        return simpleDesc;
    }

    public int getAlertPolicy() {
        return alertPolicy;
    }

    public int getRecoverTime() {
        return this.recoverTime;
    }
}
