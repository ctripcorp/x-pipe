package com.ctrip.xpipe.redis.protocal.cmd;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.protocal.protocal.RequestStringParser;

import io.netty.channel.Channel;

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
	protected void doRequest(Channel channel) throws XpipeException {
		RequestStringParser requestString = new RequestStringParser(getName(), "no", "one");
		writeAndFlush(channel, requestString.format());
	}

}
