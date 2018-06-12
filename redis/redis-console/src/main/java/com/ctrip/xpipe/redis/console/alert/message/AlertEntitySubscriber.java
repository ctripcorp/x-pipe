package com.ctrip.xpipe.redis.console.alert.message;

import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.job.event.Subscriber;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public interface AlertEntitySubscriber extends Subscriber<AlertEntity> {
}
