package com.ctrip.xpipe.redis.console.alert.sender.email.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Mar 26, 2018
 */
public abstract class AbstractEmailSenderCallback implements AsyncEmailSenderCallback {

    protected final String EMAIL_SERVICE_CAT_TYPE = "CONSOLE.EMAIL.SERVICE";

    protected Logger logger = LoggerFactory.getLogger(getClass().getSimpleName());
}
