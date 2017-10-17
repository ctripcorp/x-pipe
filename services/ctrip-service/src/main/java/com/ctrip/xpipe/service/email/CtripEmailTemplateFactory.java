package com.ctrip.xpipe.service.email;

import com.ctrip.xpipe.api.email.EMAIL_TYPE;

/**
 * @author chen.zhu
 * <p>
 * Oct 13, 2017
 */
public class CtripEmailTemplateFactory {

    public static CtripEmailTemplate createCtripEmailTemplate(EMAIL_TYPE emailType) {
        switch (emailType) {
            case REDIS_ALERT_SEND_TO_DBA:
            case REDIS_ALERT_SEND_TO_DBA_CC_DEV:
            case REDIS_ALERT_SEND_TO_DEV:
            case REDIS_ALERT_SEND_TO_DEV_CC_DBA: {
                return new CtripAlertEmailTemplate();
            }
            default:
                break;
        }
        return null;
    }
}
