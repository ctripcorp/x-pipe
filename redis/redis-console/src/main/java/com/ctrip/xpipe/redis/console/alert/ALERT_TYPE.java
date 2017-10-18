package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.redis.console.alert.policy.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 16, 2017
 */
public enum ALERT_TYPE {

    CLIENT_INSTANCE_NOT_OK("client_status", ClientInConsisAlertPolicy.ID),
    QUORUM_DOWN_FAIL("quorum_fail", QuorumDownFailAlertPolicy.ID),
    SENTINEL_RESET("stl_rst", SentinelResetAlertPolicy.ID),
    REDIS_CONF_REWRITE_FAILURE("redis_conf_rewrite_failure", RedisConfRewriteFailAlertPolicy.ID),
    CLIENT_INCONSIS("client_inconsis", ClientInConsisAlertPolicy.ID),
    MIGRATION_MANY_UNFINISHED("migra_unfinish", MigrationManyUnfinishedAlertPolicy.ID),
    XREDIS_VERSION_NOT_VALID("xredis_version_not_valid", XRedisVersionAlertPolicy.ID),
    REDIS_REPL_DISKLESS_SYNC_ERROR("redis_repl_diskless_sync_error", ReplDiskLessAlertPolicy.ID);

    private String simpleDesc;

    private String alertPolicyId;

    ALERT_TYPE(String simpleDesc, String alertPolicyId){
        this.simpleDesc = simpleDesc;
        this.alertPolicyId = alertPolicyId;
    }

    public String simpleDesc() {
        return simpleDesc;
    }

    public String getAlertPolicyId() {
        return alertPolicyId;
    }
}
