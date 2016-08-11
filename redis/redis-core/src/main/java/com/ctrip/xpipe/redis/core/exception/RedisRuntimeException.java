package com.ctrip.xpipe.redis.core.exception;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.exception.XpipeRuntimeException;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午3:00:24
 */
public class RedisRuntimeException extends XpipeRuntimeException{

	private static final long serialVersionUID = -2100200337989565310L;

	public RedisRuntimeException(String message) {
		super(message);
	}

	public RedisRuntimeException(String message, Throwable th){
		super(message, th);
	}

	public <T extends Enum<T>> RedisRuntimeException(ErrorMessage<T> errorMessage, Throwable th){
		super(errorMessage, th);
	}
}
