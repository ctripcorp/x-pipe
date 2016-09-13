package com.ctrip.xpipe.redis.keeper.impl;

import java.util.Date;

import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperException;

/**
 * @author wenchao.meng
 *
 * Aug 25, 2016
 */
public class SetRdbDumperException extends RedisKeeperException{

	private static final long serialVersionUID = 1L;

	public SetRdbDumperException(RdbDumper oldDumper) {
		super(String.format("already exist, old dumper:%s", oldDumper));
	}

	public SetRdbDumperException(long lastTime, long minInterval) {
		super(String.format("lastDumpTime:%s, minInterval:%d", new Date(lastTime), minInterval));
	}

}
