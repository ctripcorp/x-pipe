package com.ctrip.xpipe.redis.console.alert.policy;

import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.sender.Sender;

import java.util.LinkedList;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
public class DefaultAlertPolicy implements AlertPolicy {
    @Override
    public List<String> queryRecipients() {
        return new LinkedList<>();
    }

    @Override
    public List<AlertChannel> queryChannels() {
        return new LinkedList<>();
    }

    @Override
    public int querySuspendMinute() {
        return 100;
    }

    @Override
    public int queryRecoverMinute() {
        return 100;
    }

    @Override
    public List<String> queryCCers() {
        return new LinkedList<>();
    }
}
