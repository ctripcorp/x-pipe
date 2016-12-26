package com.ctrip.xpipe.service.migration;

import com.ctrip.xpipe.api.config.Config;

/**
 * @author shyin
 *
 * Dec 22, 2016
 */
public enum MigrationPublishServiceConfig {
	INSTANCE;
	
	public static final String KEY_CREDIS_SERVEICE_ADDRESS = "credis.service.address";
	
	public Config config = Config.DEFAULT;
	
	public String getCredisServiceAddress() {
		return config.get(KEY_CREDIS_SERVEICE_ADDRESS, "localhost:8080");
	}
}
