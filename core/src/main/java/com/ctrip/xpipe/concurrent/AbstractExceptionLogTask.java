package com.ctrip.xpipe.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.exception.ExceptionUtils;

/**
 * @author wenchao.meng
 *
 * Aug 31, 2016
 */
public abstract class AbstractExceptionLogTask implements Runnable{
	
	private Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public void run() {
		try{
			doRun();
		}catch(Throwable th){
			ExceptionUtils.logException(logger, th, "[run]");
		}
	}
	
	protected abstract void doRun() throws Exception;

}
