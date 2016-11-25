package com.ctrip.xpipe.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Nov 25, 2016
 */
public class ExceptionLogWrapper {
	
	private Logger logger = LoggerFactory.getLogger(ExceptionLogWrapper.class);
	
	public void execute(Runnable run){
		
		try{
			run.run();
		}catch(Throwable th){
			logger.error("[execute]", th);
		}
	}

}
