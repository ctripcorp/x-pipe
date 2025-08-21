package com.ctrip.xpipe.redis.console.aop;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import jakarta.annotation.PostConstruct;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.ContainerLoader;

/**
 * @author shyin
 *
 * Aug 29, 2016
 */
@Aspect
@Component
public class DalTransactionAspect {

	private TransactionManager transactionManager;

	private static Logger logger = LoggerFactory.getLogger(DalTransaction.class);

	@Autowired
	private ConsoleConfig config;
	
	@PostConstruct
	private void postConstruct() {
		try {
			transactionManager = ContainerLoader.getDefaultContainer().lookup(TransactionManager.class);
			logger.info("[postConstruct]Load TransactionManager: {}", transactionManager.getClass());
		} catch (ComponentLookupException e) {
			throw new ServerException("Cannot find transaction manager.",e);
		}
	}
	
	@Pointcut("@annotation(com.ctrip.xpipe.redis.console.annotation.DalTransaction)")
	public void dalTransaction(){
	}
	
	@Around("dalTransaction()")
	public Object invokeDalTransactionMethod(ProceedingJoinPoint joinPoint) {
		String datasource = config.getDatasource();
		if(null == datasource) {
			throw new ServerException("Cannot fetch datasource.");
		}
		logger.info("[invokeDalTransactionMethod] TransactionManager: {}", transactionManager.getClass());
		transactionManager.startTransaction(datasource);
		
		Object result;
		try {
			result = joinPoint.proceed();
			transactionManager.commitTransaction();
			return result;
		} catch (Throwable e) {
			transactionManager.rollbackTransaction();
			throw new ServerException(e.getMessage(), e);
		}
		
	}
	
	
}
