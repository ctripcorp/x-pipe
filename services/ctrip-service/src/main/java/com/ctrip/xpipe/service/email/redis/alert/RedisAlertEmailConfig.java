package com.ctrip.xpipe.service.email.redis.alert;

import com.ctrip.xpipe.config.AbstractConfigBean;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2017
 */
public class RedisAlertEmailConfig extends AbstractConfigBean {

    private static final String KEY_DBA_EMAILS = "redis.dba.emails";
    private static final String KEY_CC_EMAILS = "redis.alert.cc.emails";
    private static final String KEY_SENDER_EMAIL = "redis.alert.sender.email";
    private static final String KEY_ENVIRONMENT = "xpipe.runtime.environment";

    public String getDBAEmails() {
        return getProperty(KEY_DBA_EMAILS, "");
    }

    public String getCCEmails() {
        return getProperty(KEY_CC_EMAILS, "");
    }

    public String getSenderEmail() {
        return getProperty(KEY_SENDER_EMAIL, "XPipe@ctrip.com");
    }

    public String getEnvironment() {
        return getProperty(KEY_ENVIRONMENT, "");
    }
}
