package com.ctrip.xpipe.redis.keeper.protocal.cmd;



import com.ctrip.xpipe.redis.keeper.protocal.protocal.RequestStringParser;

import io.netty.buffer.ByteBuf;

/**
 * @author marsqing
 *
 *         May 9, 2016 5:42:01 PM
 */
public class InfoCommand extends AbstractRedisCommand {

	private String args;

	/**
	 * @param channel
	 */
	public InfoCommand() {
		this("");
	}

	public InfoCommand(String args) {
		this.args = args;
	}

	@Override
	public String getName() {
		return "info";
	}

	@Override
	protected ByteBuf doRequest(){
		RequestStringParser requestString = new RequestStringParser(getName(), args);
		return requestString.format();
	}

}
