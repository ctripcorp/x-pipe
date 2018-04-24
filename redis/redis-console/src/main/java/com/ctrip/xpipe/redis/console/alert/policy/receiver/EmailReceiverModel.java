package com.ctrip.xpipe.redis.console.alert.policy.receiver;

import com.ctrip.xpipe.tuple.Pair;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Apr 20, 2018
 */
public class EmailReceiverModel extends Pair<List<String>, List<String>> {

    public EmailReceiverModel(List<String> key, List<String> value) {
        super(key, value);
    }

    public EmailReceiverModel() {
    }

    public List<String> getRecipients() {
        return this.getKey();
    }

    public List<String> getCcers() {
        return this.getValue();
    }
}
