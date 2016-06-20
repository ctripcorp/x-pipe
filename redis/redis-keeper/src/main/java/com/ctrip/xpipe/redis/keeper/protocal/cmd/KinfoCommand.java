/**
 * 
 */
package com.ctrip.xpipe.redis.keeper.protocal.cmd;


import com.ctrip.xpipe.redis.keeper.protocal.protocal.RequestStringParser;

import io.netty.buffer.ByteBuf;

/**
 * @author marsqing
 *
 *         Jun 1, 2016 11:05:11 AM
 */
public class KinfoCommand extends AbstractRedisCommand {

	private String args;

	public KinfoCommand() {
		this("");
	}

	public KinfoCommand(String args) {
		this.args = args;
	}

	@Override
	public String getName() {
		return "kinfo";
	}

	@Override
	protected ByteBuf doRequest() {
		RequestStringParser requestString = new RequestStringParser(getName(), args);
		return requestString.format();
	}

}
