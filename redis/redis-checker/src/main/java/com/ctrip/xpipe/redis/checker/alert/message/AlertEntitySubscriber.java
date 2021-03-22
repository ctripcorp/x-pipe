package com.ctrip.xpipe.redis.checker.alert.message;

import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.event.Subscriber;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public interface AlertEntitySubscriber extends Subscriber<AlertEntity> {
}
