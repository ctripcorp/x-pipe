package com.ctrip.xpipe.concurrent;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Aug 31, 2016
 */
public abstract class AbstractExceptionLogTask implements Runnable{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public void run() {
		
		try{
			doRun();
		}catch(OutOfMemoryError e){
			e.printStackTrace();
			logger.error("[run]", e);
		}catch(Throwable th){
			logger.error("[run]", th);
		}
	}
	
	protected abstract void doRun() throws Exception;

}
