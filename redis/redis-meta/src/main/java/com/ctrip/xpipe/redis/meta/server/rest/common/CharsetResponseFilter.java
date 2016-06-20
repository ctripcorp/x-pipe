package com.ctrip.xpipe.redis.meta.server.rest.common;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

public class CharsetResponseFilter implements ContainerResponseFilter {

	@Override
	public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
	      throws IOException {
		MultivaluedMap<String, Object> headers = responseContext.getHeaders();
		MediaType type = responseContext.getMediaType();
		if (type != null) {
			if (!type.getParameters().containsKey(MediaType.CHARSET_PARAMETER)) {
				MediaType typeWithCharset = type.withCharset("utf-8");
				headers.putSingle("Content-Type", typeWithCharset);
			}
		}
	}
}
