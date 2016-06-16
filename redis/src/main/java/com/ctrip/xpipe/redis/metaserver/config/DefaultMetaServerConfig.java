/**
 * 
 */
package com.ctrip.xpipe.redis.metaserver.config;

import org.springframework.stereotype.Component;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 11:50:44 AM
 */
@Component
public class DefaultMetaServerConfig implements MetaServerConfig {

	@Override
	public String getZkMetaStoragePath() {
		return System.getProperty("zkMetaStoragePath", "/meta");
	}

}
