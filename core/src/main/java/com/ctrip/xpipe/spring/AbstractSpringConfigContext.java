package com.ctrip.xpipe.spring;

import com.ctrip.xpipe.exception.DefaultExceptionHandler;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.concurrent.*;

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
		Thread.setDefaultUncaughtExceptionHandler(new DefaultExceptionHandler());
	}

	public static final String SCHEDULED_EXECUTOR = "scheduledExecutor";
	public static final String GLOBAL_EXECUTOR = "globalExecutor";
	public static final int maxScheduledCorePoolSize = 8;
	public static final int maxGlobalThreads = 512;


	@Bean(name = SCHEDULED_EXECUTOR)
	public ScheduledExecutorService getScheduledExecutorService() {

		int corePoolSize = OsUtils.getCpuCount();
		if (corePoolSize > maxScheduledCorePoolSize) {
			corePoolSize = maxScheduledCorePoolSize;
		}
		return MoreExecutors.getExitingScheduledExecutorService(
				new ScheduledThreadPoolExecutor(corePoolSize, XpipeThreadFactory.create(SCHEDULED_EXECUTOR))
		);
	}


	@Bean(name = GLOBAL_EXECUTOR)
	public ExecutorService getGlobalExecutor() {
		return new ThreadPoolExecutor(10,
				maxGlobalThreads,
				120L, TimeUnit.SECONDS,
				new SynchronousQueue<>(),
				XpipeThreadFactory.create(GLOBAL_EXECUTOR),
				new ThreadPoolExecutor.CallerRunsPolicy());
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
