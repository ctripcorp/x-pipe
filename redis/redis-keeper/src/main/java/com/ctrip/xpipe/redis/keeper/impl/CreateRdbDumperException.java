package com.ctrip.xpipe.redis.keeper.impl;


import com.ctrip.xpipe.redis.keeper.RedisMaster;

/**
 * @author wenchao.meng
 *
 * Aug 25, 2016
 */
public class CreateRdbDumperException extends AbstractRdbDumperException{

	private static final long serialVersionUID = 1L;

	public CreateRdbDumperException(RedisMaster redisMaster, String reason) {
		super(String.format("redis master %s create rdbdumper fail:%s", redisMaster.toString(), reason), true);
	}

}
