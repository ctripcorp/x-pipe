package com.ctrip.xpipe.redis.checker.alert;

import com.ctrip.xpipe.redis.checker.alert.sender.email.AsyncEmailSender;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
public enum AlertChannel {
    MAIL(AsyncEmailSender.ID);

    private String id;

    public static AlertChannel findByName(String id) {
        for (AlertChannel channel : values()) {
            if (channel.getId().equals(id)) {
                return channel;
            }
        }
        return null;
    }

    AlertChannel(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
