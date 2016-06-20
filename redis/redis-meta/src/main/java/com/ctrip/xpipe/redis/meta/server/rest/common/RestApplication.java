package com.ctrip.xpipe.redis.meta.server.rest.common;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class RestApplication extends ResourceConfig {
	public RestApplication() {
		register(CharsetResponseFilter.class);
		register(CORSResponseFilter.class);
		register(ObjectMapperProvider.class);
		register(MultiPartFeature.class);
		packages("com.ctrip.xpipe.redis.meta.server.rest.resource");
	}
}
