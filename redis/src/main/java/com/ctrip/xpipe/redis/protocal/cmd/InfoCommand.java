package com.ctrip.xpipe.redis.protocal.cmd;


import com.ctrip.xpipe.exception.XpipeException;

import io.netty.channel.Channel;

/**
 * @author marsqing
 *
 *         May 9, 2016 5:42:01 PM
 */
public class InfoCommand extends AbstractRedisCommand {

	/**
	 * @param channel
	 */
	public InfoCommand() {
	}

	@Override
	public String getName() {
		return null;
	}

	@Override
	protected void doRequest(Channel channel) throws XpipeException {
		// TODO Auto-generated method stub

	}

}
