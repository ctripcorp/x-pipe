package com.ctrip.xpipe.command;

/**
 * @author wenchao.meng
 *
 * Jun 26, 2016
 */
public class CommandExecutionException extends Exception{

	private static final long serialVersionUID = 1L;
	
	
	public CommandExecutionException(String message){
		super(message);
	}

	public CommandExecutionException(String message, Throwable th){
		super(message, th);
	}

}
