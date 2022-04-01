package com.ctrip.xpipe.spring;

import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.exception.DefaultExceptionHandler;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.catalina.startup.Tomcat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.boot.web.embedded.tomcat.TomcatWebServer;
import org.springframework.boot.web.server.WebServer;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
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
	
	protected static Logger logger = LoggerFactory.getLogger(AbstractSpringConfigContext.class);
	
	public static ApplicationContext applicationContext;
	
	static{
		Thread.setDefaultUncaughtExceptionHandler(new DefaultExceptionHandler());
	}

	public static final String SCHEDULED_EXECUTOR = "scheduledExecutor";
	public static final String GLOBAL_EXECUTOR = "globalExecutor";
	public static final String CLUSTER_SHARD_ADJUST_EXECUTOR = "clusterShardAdjustExecutor";
	public static final String PEER_MASTER_CHOOSE_EXECUTOR = "peerMasterChooseExecutor";
	public static final String PEER_MASTER_ADJUST_EXECUTOR = "peerMasterAdjustExecutor";
	public static final int maxScheduledCorePoolSize = 8;
	public static final int THREAD_POOL_TIME_OUT = 5;
	public static final int GLOBAL_THREAD_MULTI_CORE = 100;
	public static final int GLOBAL_THREAD_MAX = 100;


	@Bean(name = SCHEDULED_EXECUTOR)
	public ScheduledExecutorService getScheduledExecutorService() {

		int corePoolSize = OsUtils.getCpuCount();
		if (corePoolSize > maxScheduledCorePoolSize) {
			corePoolSize = maxScheduledCorePoolSize;
		}
		return MoreExecutors.getExitingScheduledExecutorService(
				new ScheduledThreadPoolExecutor(corePoolSize, XpipeThreadFactory.create(SCHEDULED_EXECUTOR)),
				THREAD_POOL_TIME_OUT, TimeUnit.SECONDS
		);
	}


	@Bean(name = GLOBAL_EXECUTOR)
	public ExecutorService getGlobalExecutor() {

		int corePoolSize = OsUtils.getMultiCpuOrMax(GLOBAL_THREAD_MULTI_CORE, GLOBAL_THREAD_MAX);
		int maxPoolSize =  2 * OsUtils.getCpuCount();
		DefaultExecutorFactory executorFactory = new DefaultExecutorFactory(GLOBAL_EXECUTOR, corePoolSize, maxPoolSize,
				new ThreadPoolExecutor.AbortPolicy());
		return executorFactory.createExecutorService();
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

	public static Executor tryGetTomcatHttpExecutor() {
		if (!(applicationContext instanceof ServletWebServerApplicationContext))  {
			logger.info("[tryGetTomcatHttpExecutor][unexpected context] {}", null != applicationContext ?
					applicationContext.getClass().getName() : null);
			return null;
		}

		ServletWebServerApplicationContext webApplicationContext = (ServletWebServerApplicationContext) applicationContext;
		WebServer webServer = webApplicationContext.getWebServer();
		if (!(webServer instanceof TomcatWebServer)) {
			logger.info("[tryGetTomcatHttpExecutor][unexpected server] {}", null != webServer ?
					webServer.getClass().getName() : null);
			return null;
		}

		Tomcat tomcat = ((TomcatWebServer) webServer).getTomcat();
		if (0 == tomcat.getService().findConnectors().length) {
			logger.info("[tryGetTomcatHttpExecutor][connector not inited]");
			return null;
		}

		return tomcat.getConnector().getProtocolHandler().getExecutor();
	}

}
