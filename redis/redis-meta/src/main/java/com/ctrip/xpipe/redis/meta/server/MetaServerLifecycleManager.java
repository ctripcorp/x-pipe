package com.ctrip.xpipe.redis.meta.server;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.lifecycle.SpringComponentRegistry;

/**
 * @author wenchao.meng
 *
 * Jun 17, 2016
 */
@Component
public class MetaServerLifecycleManager implements ApplicationContextAware{
	
	private ComponentRegistry componentRegistry;
	private static ApplicationContext applicationContext;
	private Logger logger = LoggerFactory.getLogger(MetaServerLifecycleManager.class);
	
	//for test
	public static String META_SERVER_START_KEY = "metaServerStart"; 
	private boolean  metaServerStart = Boolean.parseBoolean(System.getProperty("metaServerStart", "true"));
	
	@PostConstruct
	public void startAll() throws Exception{

		if(!metaServerStart){
			logger.warn("[startAll][no return]");
			metaServerStart = true;
			return;
		}
		componentRegistry.initialize();
		componentRegistry.start();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		
		MetaServerLifecycleManager.applicationContext = applicationContext;
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
