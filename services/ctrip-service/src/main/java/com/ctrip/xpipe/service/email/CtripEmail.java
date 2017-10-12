package com.ctrip.xpipe.service.email;

import com.ctrip.xpipe.api.email.Email;

import java.util.LinkedList;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 12, 2017
 */
public class CtripEmail implements Email, CtripEmailTemplate {

    private static final int APP_ID = 100004374;

    private static final int BODY_TEMPLATE_ID = 37030053;

    private static final String SEND_CODE = "37030053";

    private static final String UTF_8 = "UTF-8";

    private List<String> recipients = new LinkedList<>();
    private List<String> cCers = new LinkedList<>();
    private List<String> bCCers = new LinkedList<>();
    private String sender;
    private String charset = UTF_8;
    private String subject;
    private String bodyContent;

    @Override
    public List<String> getRecipients() {
        return recipients;
    }

    @Override
    public List<String> getCCers() {
        return cCers;
    }

    @Override
    public List<String> getBCCers() {
        return bCCers;
    }

    @Override
    public String getSender() {
        return sender;
    }

    @Override
    public void addRecipient(String emailAddr) {
        recipients.add(emailAddr);
    }

    @Override
    public void addCCer(String emailAddr) {
        cCers.add(emailAddr);
    }

    @Override
    public void addBCCer(String emailAddr) {
        bCCers.add(emailAddr);
    }

    @Override
    public String getSubject() {
        return subject;
    }

    @Override
    public String getCharset() {
        return charset;
    }

    @Override
    public String getBodyContent() {
        return bodyContent;
    }

    @Override
    public void setSender(String sender) {
        this.sender = sender;
    }

    @Override
    public void setSubject(String subject) {
        this.subject = subject;
    }

    @Override
    public void setCharset(String charset) {
        this.charset = charset;
    }

    @Override
    public void setBodyContent(String bodyContent) {
        this.bodyContent = bodyContent;
    }

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
        return true;
    }

    @Override
    public String getSendCode() {
        return SEND_CODE;
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
