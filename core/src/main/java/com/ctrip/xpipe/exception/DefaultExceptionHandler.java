package com.ctrip.xpipe.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;

/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午5:37:52
 */
public class DefaultExceptionHandler implements UncaughtExceptionHandler{
	
	protected Logger logger = LoggerFactory.getLogger(DefaultExceptionHandler.class);

	@Override
	public void uncaughtException(Thread t, Throwable e) {

		System.err.println(String.format("currentThread:%s, thread:%s" , Thread.currentThread(), t));
		e.printStackTrace();

		logger.error(String.format("currentThread:%s, thread:%s" , Thread.currentThread(), t), e);
	}
}
