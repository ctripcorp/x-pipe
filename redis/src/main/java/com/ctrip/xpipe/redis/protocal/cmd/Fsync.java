package com.ctrip.xpipe.redis.protocal.cmd;

import com.ctrip.xpipe.redis.protocal.protocal.RequestStringParser;

import io.netty.buffer.ByteBuf;

/**
 * @author marsqing
 *
 *         May 16, 2016 6:46:15 PM
 */
public class Fsync extends AbstractRedisCommand {

	@Override
	public String getName() {
		return "fsync";
	}

	@Override
	protected ByteBuf doRequest(){
		RequestStringParser requestString = new RequestStringParser(getName());
		return requestString.format();
	}

}