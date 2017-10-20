package com.ctrip.xpipe.api.email;

import java.util.LinkedList;
import java.util.List;

/**
 * @author chen.zhu
 *
 * Oct 09, 2017
 */

public class Email {

    private List<String> recipients;
    private List<String> cCers;
    private List<String> bCCers;
    private String sender;
    private String charset;
    private String subject;
    private String bodyContent;
    private EmailType emailType;

    public Email() {
        recipients = new LinkedList<>();
        cCers = new LinkedList<>();
        bCCers = new LinkedList<>();
        charset = "UTF-8";
    }

    public void setRecipients(List<String> recipients) {
        this.recipients = recipients;
    }

    public void setcCers(List<String> cCers) {
        this.cCers = cCers;
    }

    public void setbCCers(List<String> bCCers) {
        this.bCCers = bCCers;
    }

    public EmailType getEmailType() {
        return emailType;
    }

    public void setEmailType(EmailType emailType) {
        this.emailType = emailType;
    }

    public List<String> getRecipients() {
        return recipients;
    }

    public List<String> getCCers() {
        return cCers;
    }

    public List<String> getBCCers() {
        return bCCers;
    }

    public String getSender() {
        return sender;
    }

    public void addRecipient(String emailAddr) {
        recipients.add(emailAddr);
    }

    public void addCCer(String emailAddr) {
        cCers.add(emailAddr);
    }

    public void addBCCer(String emailAddr) {
        bCCers.add(emailAddr);
    }

    public String getSubject() {
        return subject;
    }

    public String getCharset() {
        return charset;
    }

    public String getBodyContent() {
        return bodyContent;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public void setBodyContent(String bodyContent) {
        this.bodyContent = bodyContent;
    }
}
