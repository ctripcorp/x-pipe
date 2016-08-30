package com.ctrip.xpipe.spring;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.HandlerInterceptor;

import com.ctrip.xpipe.exception.GlobalExceptionHandler;
import com.ctrip.xpipe.lifecycle.SpringComponentLifecycleManager;

/**
 * @author wenchao.meng
 *
 * Jun 25, 2016
 */
@ComponentScan("com.ctrip.xpipe.monitor")
public abstract class AbstractSpringConfigContext implements ApplicationContextAware{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	public static ApplicationContext applicationContext;
	
	static{
		Thread.setDefaultUncaughtExceptionHandler(new GlobalExceptionHandler());
	}

	@Bean
	public SpringComponentLifecycleManager getSpringComponentLifecycleManager(){
		return new SpringComponentLifecycleManager();
	}
	
	@Bean
	public HandlerExceptionResolver getHandlerExceptionResolver(){
		return new ExceptionLoggerResolver();
	}
	
	@Bean
	public HandlerInterceptor  logApiIntercept(){
		
		return new LoggingHandlerInterceptor();
	}
	
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		
		AbstractSpringConfigContext.applicationContext = applicationContext;
	}
	
	
	public static ApplicationContext getApplicationContext() {
		return applicationContext;
	}

}
