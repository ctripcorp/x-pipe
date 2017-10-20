package com.ctrip.xpipe.redis.console.alert.policy;

import com.ctrip.xpipe.redis.console.alert.AlertChannel;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
public interface AlertPolicy {

    List<String> queryRecipients();

    List<String> queryCCers();

    List<AlertChannel> queryChannels();

    int querySuspendMinute();

    int queryRecoverMinute();

}
