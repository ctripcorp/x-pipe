package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.api.email.EMAIL_TYPE;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 16, 2017
 */
public enum ALERT_TYPE {

    CLIENT_INSTANCE_NOT_OK("client_status", EMAIL_TYPE.REDIS_ALERT_SEND_TO_DBA_CC_DEV),
    QUORUM_DOWN_FAIL("quorum_fail", EMAIL_TYPE.REDIS_ALERT_SEND_TO_DEV),
    SENTINEL_RESET("stl_rst", EMAIL_TYPE.DO_NOTHING),
    REDIS_CONF_REWRITE_FAILURE("redis_conf_rewrite_failure", EMAIL_TYPE.REDIS_ALERT_SEND_TO_DBA_CC_DEV),
    CLIENT_INCONSIS("client_inconsis", EMAIL_TYPE.REDIS_ALERT_SEND_TO_DBA_CC_DEV),
    MIGRATION_MANY_UNFINISHED("migra_unfinish", EMAIL_TYPE.REDIS_ALERT_SEND_TO_DEV),
    XREDIS_VERSION_NOT_VALID("xredis_version_not_valid", EMAIL_TYPE.REDIS_ALERT_SEND_TO_DBA_CC_DEV),
    REDIS_REPL_DISKLESS_SYNC_ERROR("redis_repl_diskless_sync_error", EMAIL_TYPE.REDIS_ALERT_SEND_TO_DBA_CC_DEV);

    private String simpleDesc;

    private EMAIL_TYPE emailType;

    ALERT_TYPE(String simpleDesc, EMAIL_TYPE emailType){
        this.simpleDesc = simpleDesc;
        this.emailType = emailType;
    }

    public String simpleDesc() {
        return simpleDesc;
    }

    public EMAIL_TYPE emailType() {
        return emailType;
    }
}
