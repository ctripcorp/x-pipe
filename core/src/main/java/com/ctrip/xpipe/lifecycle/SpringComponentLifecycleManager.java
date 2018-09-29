package com.ctrip.xpipe.lifecycle;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.*;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;

/**
 * @author wenchao.meng
 *
 * Jun 17, 2016
 */
public class SpringComponentLifecycleManager implements ApplicationContextAware, ApplicationListener<ApplicationEvent>{

	private Logger logger = LoggerFactory.getLogger(SpringComponentLifecycleManager.class);

	private ComponentRegistry componentRegistry;
	private static ConfigurableApplicationContext applicationContext;
	
	
	public void startAll() throws Exception{

		componentRegistry.initialize();
		componentRegistry.start();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		
		SpringComponentLifecycleManager.applicationContext = (ConfigurableApplicationContext)applicationContext;
		componentRegistry = new SpringComponentRegistry((ConfigurableApplicationContext)applicationContext);
	}
	
	public void stopAll() throws Exception{

		componentRegistry.stop();
		componentRegistry.dispose();
	}
	
	public static ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	@Override
	public void onApplicationEvent(ApplicationEvent event) {
		
		if(event instanceof ContextRefreshedEvent){
			try {
				logger.info("[onApplicationEvent][ContextRefreshedEvent, startAll]");
				startAll();
			} catch (Exception e) {
				throw new XpipeRuntimeException("[startAll][fail]", e);
			}
		}
		
		if(event instanceof ContextClosedEvent){
			try {
				logger.info("[onApplicationEvent][ContextClosedEvent, stopAll]");
				stopAll();
			} catch (Exception e) {
				logger.error("[onApplicationEvent][stop all]", e);
				throw new XpipeRuntimeException("[stopAll][fail]", e);
			}
		}
	}
}
