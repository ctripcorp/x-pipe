package com.ctrip.xpipe.email;

import com.ctrip.xpipe.api.email.Email;

import java.util.LinkedList;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2017
 */
public class DefaultEmail implements Email {

    List<String> recipients = new LinkedList<>();
    List<String> cCers = new LinkedList<>();
    List<String> bCCers = new LinkedList<>();
    String sender;

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
    public int getOrder() {
        return LOWEST_PRECEDENCE;
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
}
