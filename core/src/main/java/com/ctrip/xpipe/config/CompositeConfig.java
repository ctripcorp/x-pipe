package com.ctrip.xpipe.config;

import com.ctrip.xpipe.api.config.Config;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *
 * Aug 9, 2016
 */
public class CompositeConfig extends AbstractConfig{

	private List<Config> configs = new LinkedList<>();

	public CompositeConfig(Config ... configsArgu) {
		for(Config config : configsArgu){
			configs.add(config);
		}
	}
	
	public void addConfig(Config config){
		
		configs.add(config);
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
