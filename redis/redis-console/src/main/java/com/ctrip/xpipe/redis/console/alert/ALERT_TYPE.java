package com.ctrip.xpipe.redis.console.alert;

import static com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManager.EMAIL_DBA;
import static com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManager.EMAIL_XPIPE_ADMIN;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 16, 2017
 */
public enum ALERT_TYPE {

    CLIENT_INSTANCE_NOT_OK("client_status", EMAIL_DBA | EMAIL_XPIPE_ADMIN, 1),
    QUORUM_DOWN_FAIL("quorum_fail", EMAIL_XPIPE_ADMIN, 1),
    SENTINEL_RESET("stl_rst", EMAIL_DBA | EMAIL_XPIPE_ADMIN, 1),
    REDIS_CONF_REWRITE_FAILURE("redis_conf_rewrite_failure", EMAIL_DBA|EMAIL_XPIPE_ADMIN, 1),
    CLIENT_INCONSIS("client_inconsis", EMAIL_DBA | EMAIL_XPIPE_ADMIN, 1),
    MIGRATION_MANY_UNFINISHED("migra_unfinish", EMAIL_XPIPE_ADMIN, 1),
    XREDIS_VERSION_NOT_VALID("xredis_version_not_valid", EMAIL_DBA, 1),
    REDIS_REPL_DISKLESS_SYNC_ERROR("redis_repl_diskless_sync_error", EMAIL_DBA, 1);

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
