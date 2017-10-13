package com.ctrip.xpipe.service.email;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2017
 */
public interface CtripEmailTemplate {

    Integer getAppID();
    Integer getBodyTemplateID();
    boolean isBodyHTML();
    String getSendCode();
}
