package com.ctrip.xpipe.exception;

import java.lang.Thread.UncaughtExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Aug 16, 2016
 */
public class GlobalExceptionHandler implements UncaughtExceptionHandler{
	
	private static Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		
		logger.error(String.format("currentThread:%s, thread:%s" , Thread.currentThread(), t), e);
	}

}
