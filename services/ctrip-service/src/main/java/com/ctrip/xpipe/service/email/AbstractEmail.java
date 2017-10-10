package com.ctrip.xpipe.service.email;

import com.ctrip.xpipe.api.email.Email;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2017
 */
public abstract class AbstractEmail implements Email, CtripEmailTemplate{
    @Override
    public List<String> getRecipients() {
        return null;
    }

    @Override
    public List<String> getCCers() {
        return null;
    }

    @Override
    public List<String> getBCCers() {
        return null;
    }

    @Override
    public String getSender() {
        return null;
    }

    @Override
    public Integer getAppID() {
        return null;
    }

    @Override
    public Integer getBodyTemplateID() {
        return null;
    }

    @Override
    public boolean isBodyHTML() {
        return true;
    }

    @Override
    public String getSendCode() {
        return null;
    }

    @Override
    public String getSubject() {
        return null;
    }

    @Override
    public String getCharset() {
        return null;
    }

    @Override
    public String getBodyContent(Object... context) {
        return null;
    }
}
