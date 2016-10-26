package com.ctrip.xpipe.redis.console.aop;

import javax.annotation.PostConstruct;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.exception.ServerException;


/**
 * @author shyin
 *
 * Aug 29, 2016
 */
@Aspect
@Component
public class DalTransactionAspect {
	private TransactionManager transactionManager;
	
	@Autowired
	private ConsoleConfig config;
	
	@PostConstruct
	private void postConstruct() {
		try {
			transactionManager = ContainerLoader.getDefaultContainer().lookup(TransactionManager.class);
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
