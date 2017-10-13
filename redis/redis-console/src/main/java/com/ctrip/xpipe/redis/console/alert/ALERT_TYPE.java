package com.ctrip.xpipe.redis.console.alert;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 16, 2017
 */
public enum ALERT_TYPE {

    CLIENT_INSTANCE_NOT_OK("client_status"),
    QUORUM_DOWN_FAIL("quorum_fail"),
    SENTINEL_RESET("stl_rst"),
    REDIS_CONF("redis_conf"),
    CLIENT_INCONSIS("client_inconsis"),
    MIGRATION_MANY_UNFINISHED("migra_unfinish"),
    REDIS_VERSION_NOT_VALID("redis_version_not_valid"),
    REDIS_CONF_NOT_VALID("redis_conf_not_valid");

    private String simpleDesc;

    ALERT_TYPE(String simpleDesc){
        this.simpleDesc = simpleDesc;
    }

    public String simpleDesc() {
        return simpleDesc;
    }
}
