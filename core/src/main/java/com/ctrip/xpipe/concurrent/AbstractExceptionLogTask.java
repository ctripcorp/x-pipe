package com.ctrip.xpipe.concurrent;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Aug 31, 2016
 */
public abstract class AbstractExceptionLogTask implements Runnable{
	
	private Logger logger;

	@Override
	public void run() {
		
		try{
			doRun();
		}catch(OutOfMemoryError e){
			e.printStackTrace();
			getLogger().error("[run]", e);
		}catch(Throwable th){
			getLogger().error("[run]", th);
		}
	}
	
	protected abstract void doRun() throws Exception;

	protected Logger getLogger() {
		//won't be necessary to lock, as logger factory will guarantee the singleton in multi-thread
		if(logger == null) {
			logger = LoggerFactory.getLogger(getClass());
		}
		return logger;
	}

}
