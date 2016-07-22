package com.ctrip.xpipe.api.config;

import com.ctrip.xpipe.utils.ServicesUtil;

/**
 * @author wenchao.meng
 *
 * Jul 21, 2016
 */
public interface Config {
	
	public static Config DEFAULT = ServicesUtil.getConfigService();

	String get(String key);

	void addConfigChangeListener(ConfigChangeListener configChangeListener);

	void removeConfigChangeListener(ConfigChangeListener configChangeListener);
}
