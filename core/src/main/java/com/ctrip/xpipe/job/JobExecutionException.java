package com.ctrip.xpipe.job;

/**
 * @author wenchao.meng
 *
 * Jun 26, 2016
 */
public class JobExecutionException extends Exception{

	private static final long serialVersionUID = 1L;
	
	
	public JobExecutionException(String message){
		super(message);
	}

	public JobExecutionException(String message, Throwable th){
		super(message, th);
	}

}
