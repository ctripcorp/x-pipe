package com.ctrip.xpipe.exception;

import com.ctrip.xpipe.command.CommandTimeoutException;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;

import java.net.SocketException;

/**
 * @author wenchao.meng
 *
 * Aug 28, 2016
 */
public class ExceptionUtils {
	
	public static Throwable getRootCause(Throwable th){
		
		if(th == null){
			return null;
		}
		
		Throwable cause = th.getCause();
		if(cause == null){
			return th;
		}
		
		return getRootCause(cause);
	}
	
	public static boolean isSocketIoException(Throwable th){
		
		while(true){
			
			if(th == null){
				break;
			}
			if(th instanceof SocketException){
				return true;
			}
			th = th.getCause();
		}
		
		return false;
	}

	public static String extractExtraMessage(Throwable throwable){

		Throwable rootExeption = getRootCause(throwable);
		
		if(rootExeption instanceof HttpStatusCodeException){
			return "response body:" + ((HttpStatusCodeException) rootExeption).getResponseBodyAsString();
		}
		
		return null;
	}

	public static boolean xpipeExceptionLogMessage(Throwable throwable) {
		
		if(throwable instanceof XpipeException){
			return ((XpipeException) throwable).isOnlyLogMessage();
		}
		if(throwable instanceof XpipeRuntimeException){
			return ((XpipeRuntimeException) throwable).isOnlyLogMessage();
		}
		return false;
	}

	public static boolean isStackTraceUnnecessary(Throwable throwable) {
		return ExceptionUtils.getRootCause(throwable) instanceof CommandTimeoutException
				|| throwable instanceof ResourceAccessException;
	}
}
