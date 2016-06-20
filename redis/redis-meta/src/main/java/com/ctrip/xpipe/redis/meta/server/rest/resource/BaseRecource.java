package com.ctrip.xpipe.redis.meta.server.rest.resource;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;

import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * @author marsqing
 *
 *         May 26, 2016 6:34:49 PM
 */
public class BaseRecource {

	@Context
	protected ServletContext servletContext;

	protected WebApplicationContext getSpringContext() {
		return WebApplicationContextUtils.getWebApplicationContext(servletContext);
	}

}
