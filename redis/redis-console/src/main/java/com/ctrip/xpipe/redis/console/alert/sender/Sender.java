package com.ctrip.xpipe.redis.console.alert.sender;

import com.ctrip.xpipe.redis.console.alert.AlertMessageEntity;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
public interface Sender {
    String getId();
    boolean send(AlertMessageEntity message);
}
