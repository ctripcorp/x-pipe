package com.ctrip.xpipe.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.config.ConfigChangeListener;

/**
 * @author wenchao.meng
 *
 * Jul 22, 2016
 */
public abstract class AbstractConfigBean implements ConfigChangeListener {
	
	private Config config;
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	public AbstractConfigBean(){
		this(Config.DEFAULT);
	}

	public AbstractConfigBean(Config config){
		this.config = config;
		config.addConfigChangeListener(this);
	}

	protected void setConfig(Config config) {
		this.config = config;
	}

	protected String getProperty(String key){
		return config.get(key);
	}

	protected String getProperty(String key, String defaultValue){
		return config.get(key, defaultValue);
	}

	protected Integer getIntProperty(String key, Integer defaultValue){
		
		String value = config.get(key);
		if(value == null){
			return defaultValue;
		}
		return Integer.parseInt(value.trim());
		
	}

	protected Long getLongProperty(String key, Long defaultValue){
		
		String value = config.get(key);
		if(value == null){
			return defaultValue;
		}
		return Long.parseLong(value.trim());
		
	}

	protected Boolean getBooleanProperty(String key, Boolean defaultValue){
		
		String value = config.get(key);
		if(value == null){
			return defaultValue;
		}
		
		return Boolean.parseBoolean(value.trim());
	}
	
	@Override
	public void onChange(String key, String oldValue, String newValue) {
		
	}
	
	@Override
	public String toString() {
		return Codec.DEFAULT.encode(this);
	}

}
