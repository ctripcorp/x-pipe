package com.ctrip.xpipe.redis.console.alert.sender;

import com.ctrip.xpipe.redis.console.alert.manager.SenderManager;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
public abstract class AbstractSender implements Sender {

    @Autowired
    private SenderManager senderManager;

    public Sender querySender() {
        String id = getId();
        return senderManager.querySender(id);
    }
}
