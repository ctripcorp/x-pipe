package com.ctrip.xpipe.redis.console.rest.resource;


import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.ctrip.xpipe.redis.console.dao.MetaDao;
import com.ctrip.xpipe.redis.console.service.MetaService;

/**
 * @author marsqing
 *
 *         May 26, 2016 6:34:49 PM
 */
public class BaseRecource {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Context
	protected ServletContext servletContext;

	protected WebApplicationContext getSpringContext() {
		return WebApplicationContextUtils.getWebApplicationContext(servletContext);
	}
	
	protected MetaDao getMetaDao(){
		
		return getSpringContext().getBean(MetaDao.class);
	}
	
	
	protected MetaService getMetaService(){
		
		return getSpringContext().getBean(MetaService.class);
	}

}
