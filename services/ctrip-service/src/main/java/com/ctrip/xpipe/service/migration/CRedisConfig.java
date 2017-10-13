package com.ctrip.xpipe.service.migration;

import java.util.Map;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.config.Config;

/**
 * @author shyin
 *
 * Dec 22, 2016
 */
public enum CRedisConfig {
	INSTANCE;
	
	public static final String KEY_CREDIS_SERVEICE_ADDRESS = "credis.service.address";
	public static final String KEY_CREDIS_IDC_MAPPING_RULE = "credis.service.idc.mapping.rule";
	
	public Config config = Config.DEFAULT;
	
	public String getCredisServiceAddress() {
		return config.get(KEY_CREDIS_SERVEICE_ADDRESS, "localhost:8080");
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, String> getCredisIdcMappingRules() {
		return Codec.DEFAULT.decode(config.get(KEY_CREDIS_IDC_MAPPING_RULE, "{}"), Map.class);
	}
}
