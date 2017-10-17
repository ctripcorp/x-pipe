package com.ctrip.xpipe.api.email;

/**
 * @author chen.zhu
 * <p>
 * Oct 13, 2017
 */
public enum EMAIL_TYPE {

    SEND_TO_DBA("send_to_dba"),
    SEND_TO_DEV("send_to_dev"),
    SEND_TO_DBA_CC_DEV("send_to_dba_cc_dev"),
    SEND_TO_DEV_CC_DBA("send_to_dev_cc_dba"),
    DO_NOTHING("do nothing");

    private String simpleDesc;

    EMAIL_TYPE(String simpleDesc){
        this.simpleDesc = simpleDesc;
    }

    public String simpleDesc() {
        return simpleDesc;
    }
}
