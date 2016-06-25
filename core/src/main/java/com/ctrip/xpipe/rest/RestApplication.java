package com.ctrip.xpipe.rest;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class RestApplication extends ResourceConfig {
	public RestApplication(String packages) {
		register(CharsetResponseFilter.class);
		register(CORSResponseFilter.class);
		register(ObjectMapperProvider.class);
		register(MultiPartFeature.class);
		packages(packages);
	}
}
