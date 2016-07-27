package com.ctrip.xpipe.api.config;

/**
 * @author wenchao.meng
 *
 * Jul 21, 2016
 */
public interface ConfigChangeListener {
	
	void onChange(String key, String oldValue, String newValue);
	
}
