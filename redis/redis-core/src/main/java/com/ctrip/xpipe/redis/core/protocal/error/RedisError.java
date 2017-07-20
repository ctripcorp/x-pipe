package com.ctrip.xpipe.redis.core.protocal.error;

import com.ctrip.xpipe.netty.commands.ProtocalErrorResponse;
import com.ctrip.xpipe.redis.core.exception.RedisException;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午5:25:46
 */
public class RedisError extends RedisException implements ProtocalErrorResponse{

	private static final long serialVersionUID = 1L;

	public RedisError(String message) {
		super(message);
	}

	public String errorMessage(){
		return super.getMessage();
	}
}
