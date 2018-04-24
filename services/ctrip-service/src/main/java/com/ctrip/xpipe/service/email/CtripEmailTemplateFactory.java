package com.ctrip.xpipe.service.email;

/**
 * @author chen.zhu
 * <p>
 * Oct 13, 2017
 */
public class CtripEmailTemplateFactory {

    public static CtripEmailTemplate createCtripEmailTemplate() {
        return new CtripAlertEmailTemplate();
    }
}
