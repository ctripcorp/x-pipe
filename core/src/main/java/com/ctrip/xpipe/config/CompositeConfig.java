package com.ctrip.xpipe.config;

import com.ctrip.xpipe.api.config.Config;

/**
 * @author wenchao.meng
 *
 * Aug 9, 2016
 */
public class CompositeConfig extends AbstractConfig{

	private Config []configs;

	public CompositeConfig(Config ... configs) {
		this.configs = configs;
	}
	
	@Override
	public String get(String key) {
		
		for(Config config : configs){
			String value = config.get(key); 
			if( value != null){
				return value;
			}
		}
		return null;
	}

}
