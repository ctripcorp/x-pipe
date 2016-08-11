package com.ctrip.xpipe.redis.core.exception;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.exception.XpipeException;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午3:00:24
 */
public class RedisException extends XpipeException{

	private static final long serialVersionUID = -2100200337989565310L;

	public RedisException(String message) {
		super(message);
	}
	
	public RedisException(String message, Throwable th){
		super(message, th);
	}
	
	public <T extends Enum<T>> RedisException(ErrorMessage<T> errorMessage, Throwable th){
		super(errorMessage, th);
	}
}
