package com.ctrip.xpipe.redis.meta.server;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

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
	
	@PostConstruct
	public void startAll() throws Exception{
		
		componentRegistry.initialize();
		componentRegistry.start();
		
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		componentRegistry = new SpringComponentRegistry(applicationContext);
	}
	
	@PreDestroy
	public void stopAll() throws Exception{

		componentRegistry.stop();
		componentRegistry.dispose();
	}
}
