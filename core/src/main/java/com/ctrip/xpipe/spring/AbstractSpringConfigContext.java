package com.ctrip.xpipe.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;

import com.ctrip.xpipe.lifecycle.SpringComponentLifecycleManager;

/**
 * @author wenchao.meng
 *
 * Jun 25, 2016
 */
public abstract class AbstractSpringConfigContext implements ApplicationContextAware{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	public static ApplicationContext applicationContext;

	@Bean
	public SpringComponentLifecycleManager getSpringComponentLifecycleManager(){
		return new SpringComponentLifecycleManager();
	}
	
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		
		AbstractSpringConfigContext.applicationContext = applicationContext;
	}
	
	
	public static ApplicationContext getApplicationContext() {
		return applicationContext;
	}

}
