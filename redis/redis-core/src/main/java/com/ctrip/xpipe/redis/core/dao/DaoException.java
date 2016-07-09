package com.ctrip.xpipe.redis.core.dao;

import com.ctrip.xpipe.redis.core.exception.RedisException;

/**
 * @author wenchao.meng
 *
 * Jun 27, 2016
 */
public class DaoException extends RedisException{

	private static final long serialVersionUID = 1L;

	public DaoException(String message) {
		super(message);
	}
	
	

}
