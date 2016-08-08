package com.ctrip.xpipe.config;

import com.ctrip.xpipe.api.config.Config;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class DefaultConfig extends AbstractConfig {
    private Config defaultFileConfig = new DefaultFileConfig();

    @Override
    public String get(String key) {
        // 1. load from system property
        String value = System.getProperty(key);

        // 2. load
        if (value == null) {
            value = defaultFileConfig.get(key);
        }

        return value;
    }
}
