package com.ctrip.xpipe.exception;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:58:53
 */
public class XpipeException extends Exception{

	private static final long serialVersionUID = 1L;
	
	public XpipeException(String message){
		super(message);
	}
	
	public XpipeException(String message, Throwable th){
		super(message, th);
	}

}
