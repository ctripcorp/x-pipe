package com.ctrip.xpipe.lifecycle;


import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;

/**
 * @author wenchao.meng
 *
 * Jun 17, 2016
 */
public class SpringComponentLifecycleManager implements ApplicationContextAware{
	
	private ComponentRegistry componentRegistry;
	private static ApplicationContext applicationContext;
	private Logger logger = LoggerFactory.getLogger(SpringComponentLifecycleManager.class);
	
	//for test
	public static String SPRING_COMPONENT_START_KEY = "springComponentStart"; 
	private boolean  springComponentStart = Boolean.parseBoolean(System.getProperty("springComponentStart", "true"));
	
	@PostConstruct
	public void startAll() throws Exception{

		if(!springComponentStart){
			logger.warn("[startAll][no return]");
			springComponentStart = true;
			return;
		}
		componentRegistry.initialize();
		componentRegistry.start();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		
		SpringComponentLifecycleManager.applicationContext = applicationContext;
		componentRegistry = new SpringComponentRegistry(applicationContext);
	}
	
	@PreDestroy
	public void stopAll() throws Exception{

		componentRegistry.stop();
		componentRegistry.dispose();
	}
	
	public static ApplicationContext getApplicationContext() {
		return applicationContext;
	}
}
