package com.ctrip.xpipe.exception;

import java.io.IOException;

import org.slf4j.Logger;

/**
 * @author wenchao.meng
 *
 * Aug 28, 2016
 */
public class ExceptionUtils {
	
	public static boolean isIoException(Throwable th){
		
		while(true){
			
			if(th == null){
				break;
			}
			if(th instanceof IOException){
				return true;
			}
			th = th.getCause();
		}
		
		return false;
	}

	public static void logException(Logger logger, Exception e, String info){
		
		if(isIoException(e)){
			logger.error(info + e.getMessage());
		}else{
			logger.error(info, e);
		}
	}
}
