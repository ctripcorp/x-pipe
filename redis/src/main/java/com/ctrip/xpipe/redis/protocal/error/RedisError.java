package com.ctrip.xpipe.redis.protocal.error;

import com.ctrip.xpipe.redis.exception.RedisException;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午5:25:46
 */
public class RedisError extends RedisException{

	private static final long serialVersionUID = 1L;

	public RedisError(String message) {
		super(message);
	}

}
