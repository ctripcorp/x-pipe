package com.ctrip.xpipe.redis.console.config;

import com.ctrip.xpipe.api.config.ConfigChangeListener;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Sep 12, 2018
 */
public interface ConsoleConfigListener extends ConfigChangeListener {

    List<String> supportsKeys();
}
