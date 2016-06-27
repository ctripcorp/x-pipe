package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author wenchao.meng
 *
 * Jun 27, 2016
 */
public class DaoException extends RedisConsoleException{

	private static final long serialVersionUID = 1L;

	public DaoException(String message) {
		super(message);
	}
	
	

}
