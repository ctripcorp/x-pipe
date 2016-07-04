package com.ctrip.xpipe.command;

import java.util.concurrent.TimeoutException;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class CommandTimeoutException extends TimeoutException{

	private static final long serialVersionUID = 1L;
	
	public CommandTimeoutException(String message){
		super(message);
	}
	
}
