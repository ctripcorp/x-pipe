package com.ctrip.xpipe.redis.core.protocal.cmd;




import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;

import io.netty.buffer.ByteBuf;

/**
 * @author marsqing
 *
 *         May 9, 2016 3:19:37 PM
 */
public class SlaveOfCommand extends AbstractRedisCommand {
	
	private String ip;
	private int port;
	private String param = "";

	public SlaveOfCommand() {
	}

	public SlaveOfCommand(String ip, int port, String param) {
		this.ip = ip;
		this.port = port;
		this.param = param;
	}

	@Override
	public String getName() {
		return "slaveof";
	}

	@Override
	protected ByteBuf doRequest() {
		
		RequestStringParser requestString = null;
		if(ip == null){
			requestString = new RequestStringParser(getName(), "no", "one", param);
		}else{
			requestString = new RequestStringParser(getName(), ip, String.valueOf(port), param);
		}
		return requestString.format();
	}

	@Override
	public String toString() {
		return String.format("%s %d %s", ip, port, param);
	}
}
