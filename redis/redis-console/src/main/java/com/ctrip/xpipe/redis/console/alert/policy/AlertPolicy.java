package com.ctrip.xpipe.redis.console.alert.policy;

import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
public interface AlertPolicy {

    List<String> queryRecipients(AlertEntity alert);

    List<String> queryCCers();

    List<AlertChannel> queryChannels(AlertEntity alert);

    int querySuspendMinute(AlertEntity alert);

    int queryRecoverMinute(AlertEntity alert);

}
