package com.ctrip.xpipe.exception;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;

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

	public static void logException(Logger logger, Throwable throwable){
		logException(logger, throwable, "");
	}

	public static void logException(Logger logger, Throwable throwable, String info){
		
		if(isIoException(throwable) || xpipeExceptionLogMessage(throwable)){
			logger.error(info + throwable.getMessage());
		}else{
			logger.error(info, throwable);
		}
	}
	
	public static Exception getOriginalException(Exception e) {
		if(e instanceof ExecutionException) {
			if(e.getCause() instanceof InvocationTargetException) {
				return (Exception) ((e.getCause().getCause() instanceof Exception)?e.getCause().getCause() : e.getCause());
			}
			return (Exception) ((e.getCause() instanceof Exception)?e.getCause():e);
		}
		if(e instanceof InvocationTargetException) {
			return (Exception) ((e.getCause() instanceof Exception)?e.getCause() : e);
		}
		return e;
	}

	private static boolean xpipeExceptionLogMessage(Throwable throwable) {
		if(throwable instanceof XpipeException){
			return ((XpipeException) throwable).isOnlyLogMessage();
		}
		if(throwable instanceof XpipeRuntimeException){
			return ((XpipeRuntimeException) throwable).isOnlyLogMessage();
		}
		return false;
	}
}
