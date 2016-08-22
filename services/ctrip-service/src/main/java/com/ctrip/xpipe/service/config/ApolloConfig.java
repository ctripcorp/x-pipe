package com.ctrip.xpipe.service.config;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.model.ConfigChange;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import com.ctrip.xpipe.config.AbstractConfig;

/**
 * @author wenchao.meng
 *
 * Jul 21, 2016
 */
public class ApolloConfig extends AbstractConfig implements ConfigChangeListener{
	
	private Config config = ConfigService.getAppConfig();
	
	public ApolloConfig() {
		config.addChangeListener(this);
	}

	@Override
	public String get(String key) {
		return config.getProperty(key, null);
	}

	@Override
	public void onChange(ConfigChangeEvent changeEvent) {
		
		for(String key : changeEvent.changedKeys()){
			
			ConfigChange change = changeEvent.getChange(key);
			notifyConfigChange(key, change.getOldValue(), change.getNewValue());
		}
		
	}

	@Override
	public String get(String key, String defaultValue) {
		return config.getProperty(key, defaultValue);
	}

	@Override
	public int getOrder() {
		return HIGHEST_PRECEDENCE;
	}
}
