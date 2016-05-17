package com.ctrip.xpipe.redis.protocal.cmd;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.protocal.protocal.RequestStringParser;

import io.netty.channel.Channel;

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
	protected void doRequest(Channel channel) throws XpipeException {
		RequestStringParser requestString = new RequestStringParser(getName());
		writeAndFlush(channel, requestString.format());
	}

}