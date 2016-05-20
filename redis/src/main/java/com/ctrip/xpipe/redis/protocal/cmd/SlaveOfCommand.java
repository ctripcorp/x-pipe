package com.ctrip.xpipe.redis.protocal.cmd;


import com.ctrip.xpipe.redis.protocal.protocal.RequestStringParser;

import io.netty.buffer.ByteBuf;

/**
 * @author marsqing
 *
 *         May 9, 2016 3:19:37 PM
 */
public class SlaveOfCommand extends AbstractRedisCommand {

	/**
	 * @param channel
	 */
	public SlaveOfCommand() {
	}

	@Override
	public String getName() {
		return "slaveof";
	}

	@Override
	protected ByteBuf doRequest() {
		RequestStringParser requestString = new RequestStringParser(getName(), "no", "one");
		return requestString.format();
	}

}
