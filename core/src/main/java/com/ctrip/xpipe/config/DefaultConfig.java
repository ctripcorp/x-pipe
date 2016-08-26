package com.ctrip.xpipe.config;

import com.ctrip.xpipe.api.config.Config;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class DefaultConfig extends AbstractConfig {
	
    private CompositeConfig config = new CompositeConfig();

    public DefaultConfig() {
    	
    	try{
    		Config fileConfig = new DefaultFileConfig();
    		config.addConfig(fileConfig);
    	}catch(Exception e){
    		logger.warn("load file config" + e.getMessage());
    	}
    	Config propertiConfig = new DefaultPropertyConfig();
    	config.addConfig(propertiConfig);
    	
	}

    @Override
    public String get(String key) {
    	
        return config.get(key);
    }
}
