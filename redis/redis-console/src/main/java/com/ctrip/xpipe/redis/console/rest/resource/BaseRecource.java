package com.ctrip.xpipe.redis.console.rest.resource;



import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.ctrip.xpipe.redis.console.dao.ConsoleDao;
import com.ctrip.xpipe.redis.console.service.ConsoleService;
import com.ctrip.xpipe.rest.DefaultRestProcessTemplate;
import com.ctrip.xpipe.rest.ProcessTemplate;

/**
 * @author marsqing
 *
 *         May 26, 2016 6:34:49 PM
 */
public class BaseRecource {
	
	protected ProcessTemplate template =  new DefaultRestProcessTemplate();
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Context
	protected ServletContext servletContext;

	protected WebApplicationContext getSpringContext() {
		return WebApplicationContextUtils.getWebApplicationContext(servletContext);
	}
	
	protected ConsoleDao getMetaDao(){
		
		
		return getBean(ConsoleDao.class);
	}
	
	
	private <T> T getBean(Class<T> clazz) {
		
		ApplicationContext applicationContext = getSpringContext();
		if(applicationContext == null){
			return null;
		}
		return applicationContext.getBean(clazz);
	}

	protected ConsoleService getMetaService(){
		
		return getBean(ConsoleService.class);
	}
	
}
