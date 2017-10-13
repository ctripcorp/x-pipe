package com.ctrip.xpipe.service.email;

/**
 * @author chen.zhu
 * <p>
 * Oct 13, 2017
 */
public class CtripAlertEmailTemplate implements CtripEmailTemplate {

    private static final int APP_ID = 100004374;

    private static final int BODY_TEMPLATE_ID = 37030053;

    private static final String SEND_CODE = "37030053";

    @Override
    public Integer getAppID() {
        return APP_ID;
    }

    @Override
    public Integer getBodyTemplateID() {
        return BODY_TEMPLATE_ID;
    }

    @Override
    public boolean isBodyHTML() {
        return Boolean.TRUE;
    }

    @Override
    public String getSendCode() {
        return SEND_CODE;
    }
}
