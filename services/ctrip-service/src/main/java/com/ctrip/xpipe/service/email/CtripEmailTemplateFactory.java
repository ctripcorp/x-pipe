package com.ctrip.xpipe.service.email;

import com.ctrip.xpipe.api.email.EmailType;

/**
 * @author chen.zhu
 * <p>
 * Oct 13, 2017
 */
public class CtripEmailTemplateFactory {

    public static CtripEmailTemplate createCtripEmailTemplate(EmailType emailType) {
        switch (emailType) {
            case CONSOLE_ALERT: {
                return new CtripAlertEmailTemplate();
            }
            default:
                break;
        }
        return null;
    }
}
