package com.ctrip.xpipe.redis.protocal.cmd;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.protocal.RedisClientProtocol;

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
	public InfoCommand(Channel channel) {
		super(channel);
		// TODO Auto-generated constructor stub
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected RESPONSE_STATE handleRedisResponse(RedisClientProtocol<?> redisClientProtocol) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void doRequest() throws XpipeException {
		// TODO Auto-generated method stub

	}

}
