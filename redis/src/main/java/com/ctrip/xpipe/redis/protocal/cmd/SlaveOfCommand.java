package com.ctrip.xpipe.redis.protocal.cmd;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.protocal.RedisClientProtocol;
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
	public SlaveOfCommand(Channel channel) {
		super(channel);
	}

	@Override
	public String getName() {
		return "slaveof";
	}

	@Override
	protected RESPONSE_STATE handleRedisResponse(RedisClientProtocol<?> redisClientProtocol) {
		// TODO check response and write something to client
//		String res = ((SimpleStringParser) redisClientProtocol).getPayload();
		return RESPONSE_STATE.CONTINUE;
	}

	@Override
	protected void doRequest() throws XpipeException {
		RequestStringParser requestString = new RequestStringParser(getName(), "no", "one");
		writeAndFlush(requestString.format());
	}

}
