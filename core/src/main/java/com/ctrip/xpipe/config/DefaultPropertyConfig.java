package com.ctrip.xpipe.config;

/**
 * @author wenchao.meng
 *
 * Aug 9, 2016
 */
public class DefaultPropertyConfig extends AbstractConfig{

	@Override
	public String get(String key) {
		return System.getProperty(key);
	}

}
