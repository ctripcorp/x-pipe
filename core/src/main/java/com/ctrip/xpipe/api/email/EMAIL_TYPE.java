package com.ctrip.xpipe.api.email;

/**
 * @author chen.zhu
 * <p>
 * Oct 13, 2017
 */
public enum EMAIL_TYPE {

    SEND_TO_DBA("send_to_dba"),
    SEND_TO_DEV("send_to_dev"),
    CC_DBA("redis_conf_not_valid");

    private String simpleDesc;

    EMAIL_TYPE(String simpleDesc){
        this.simpleDesc = simpleDesc;
    }

    public String simpleDesc() {
        return simpleDesc;
    }
}
