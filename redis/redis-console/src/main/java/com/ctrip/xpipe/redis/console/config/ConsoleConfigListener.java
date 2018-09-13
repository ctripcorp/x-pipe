package com.ctrip.xpipe.redis.console.config;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Sep 12, 2018
 */
public interface ConsoleConfigListener {

    void onChange(String key, String oldValue, String newValue);

    List<String> supportsKeys();
}
