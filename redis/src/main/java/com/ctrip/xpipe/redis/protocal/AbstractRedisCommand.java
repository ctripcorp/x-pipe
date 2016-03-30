package com.ctrip.xpipe.redis.protocal;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.protocal.data.ErrorParser;
import com.ctrip.xpipe.redis.protocal.data.SimpleString;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午12:04:13
 */
public abstract class AbstractRedisCommand extends AbstractCommand {
	
	protected AbstractRedisCommand(OutputStream ous, InputStream ins) {
		super(ous, ins);
	}
	protected String[] splitSpace(String buff) {
		
		return buff.split("\\s+");
	}

	@Override
	protected void doReadResponse() throws XpipeException, IOException {

		int sign = ins.read();
		switch(sign){
			case '\r':
				break;
			case '\n':
				break;
			case RedisClietProtocol.MINUS_BYTE:
				throw new ErrorParser().parse(ins).getPayload();
			case RedisClietProtocol.ASTERISK_BYTE:
			case RedisClietProtocol.DOLLAR_BYTE:
			case RedisClietProtocol.COLON_BYTE:
				throw new UnsupportedOperationException();
			case RedisClietProtocol.PLUS_BYTE:
				
				handleRedisResponse(new SimpleString().parse(ins));
				break;
			default:
				throw new RedisRuntimeException("unkonwn sign:" + (char)sign);
		}
	}

	protected abstract void handleRedisResponse(RedisClietProtocol<?> redisClietProtocol) throws IOException;

}
