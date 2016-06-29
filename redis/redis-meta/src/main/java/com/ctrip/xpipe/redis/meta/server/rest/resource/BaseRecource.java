package com.ctrip.xpipe.redis.meta.server.rest.resource;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.ctrip.xpipe.rest.DefaultRestProcessTemplate;
import com.ctrip.xpipe.rest.ProcessTemplate;

/**
 * @author marsqing
 *
 *         May 26, 2016 6:34:49 PM
 */
public class BaseRecource {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected ProcessTemplate processTemplate = new DefaultRestProcessTemplate();

	@Context
	protected ServletContext servletContext;

	protected WebApplicationContext getSpringContext() {
		return WebApplicationContextUtils.getWebApplicationContext(servletContext);
	}

	protected <T> T getBeansOfType(Class<T> clazz) {
		
		ApplicationContext applicationContext = getSpringContext();
		if(applicationContext == null){
			return null;
		}
		return applicationContext.getBean(clazz);
	}

}
