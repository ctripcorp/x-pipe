package com.ctrip.xpipe.redis.checker.alert.manager;

import com.ctrip.xpipe.redis.checker.alert.AlertChannel;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.alert.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
@Component
public class SenderManager {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private Map<String, Sender> senders;

    public Sender querySender(String id) {
        return senders.get(id);
    }

    public boolean sendAlert(AlertChannel channel, AlertMessageEntity message) {

        String channelId = channel.getId();
        Sender sender = senders.get(channelId);
        try {
            boolean result = sender.send(message);
            logger.debug("[sendAlert] Channel: {}, message: {}, send out: {}", channel, message.getTitle(), result);
            return result;
        } catch (Exception e) {
            logger.error("[sendAlert]", e);
            return false;
        }
    }

}
