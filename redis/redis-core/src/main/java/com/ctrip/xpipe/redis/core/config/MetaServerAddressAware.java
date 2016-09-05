package com.ctrip.xpipe.redis.core.config;

/**
 * @author wenchao.meng
 *
 * Sep 5, 2016
 */
public interface MetaServerAddressAware {
	
    public static final String META_SERVER_URL = "meta.server.url";

    String getMetaServerUrl();

}
