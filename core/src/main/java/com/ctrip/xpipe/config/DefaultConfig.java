package com.ctrip.xpipe.config;

import com.ctrip.xpipe.api.config.Config;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class DefaultConfig extends AbstractConfig {
	
    private Config config = new CompositeConfig(new DefaultFileConfig(), new DefaultPropertyConfig());

    @Override
    public String get(String key) {
    	
        return config.get(key);
    }
}
