package com.ctrip.xpipe.exception;

import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午5:37:52
 */
public class DefaultExceptionHandler implements UncaughtExceptionHandler{
	
	protected Logger logger = LogManager.getLogger();

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		
		System.out.println("Thread:" + t);
		e.printStackTrace();
		logger.error("[uncaughtException]" + t, e);
	}
}
