package com.ctrip.xpipe.redis.console.alert.decorator;

import com.ctrip.xpipe.redis.console.alert.AlertEntity;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
public abstract class Decorator {
    protected DateFormat m_format = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    public String generateContent(AlertEntity alert) {
        return alert.getMessage();
    }

    public abstract String generateTitle(AlertEntity alert);

    public abstract String getId();
}
