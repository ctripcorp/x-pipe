package com.ctrip.xpipe.redis.core.protocal.error;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午5:25:46
 */
public class NoMasterlinkRedisError extends RedisError{

	private static final long serialVersionUID = 1L;

	public NoMasterlinkRedisError(String message) {
		super("NOMASTERLINK " + message);
	}

	public String errorMessage(){
		return super.getMessage();
	}
}
