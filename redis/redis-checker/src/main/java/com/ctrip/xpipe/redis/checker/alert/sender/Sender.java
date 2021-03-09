package com.ctrip.xpipe.redis.checker.alert.sender;

import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
public interface Sender {
    String getId();
    boolean send(AlertMessageEntity message);
}
