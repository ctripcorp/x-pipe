package com.ctrip.xpipe.exception;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:58:53
 */
public class XpipeRuntimeException extends RuntimeException{

	private static final long serialVersionUID = 1L;
	
	public XpipeRuntimeException(String message){
		super(message);
	}
	
	public XpipeRuntimeException(String message, Throwable th){
		super(message, th);
	}

}
