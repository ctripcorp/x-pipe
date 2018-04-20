package com.ctrip.xpipe.redis.console.alert.sender.email.listener;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.utils.DateTimeUtils;

/**
 * @author chen.zhu
 * <p>
 * Mar 26, 2018
 */
public class EmailSendErrorReporter extends AbstractEmailSenderCallback {

    @Override
    public void success() {
        // ignore
    }

    @Override
    public void fail(Throwable throwable) {
        logger.error("[fail] Email fail exception: {}", throwable);
        EventMonitor.DEFAULT.logEvent(EMAIL_SERVICE_CAT_TYPE,
                String.format("%s Email send out error: %s",
                        DateTimeUtils.currentTimeAsString(), throwable.getMessage()));
    }
}
