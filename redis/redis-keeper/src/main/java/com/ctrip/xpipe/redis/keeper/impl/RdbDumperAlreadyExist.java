package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperException;

/**
 * @author wenchao.meng
 *
 * Aug 25, 2016
 */
public class RdbDumperAlreadyExist extends RedisKeeperException{

	private static final long serialVersionUID = 1L;

	public RdbDumperAlreadyExist(RdbDumper oldDumper) {
		super(String.format("old dumper:%s", oldDumper));
	}

}
