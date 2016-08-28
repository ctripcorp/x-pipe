package com.ctrip.xpipe.exception;

import java.io.IOException;

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

}
