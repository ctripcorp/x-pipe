package com.ctrip.xpipe.redis.core.protocal.protocal;



import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;

import io.netty.buffer.ByteBuf;


/**
 * @author wenchao.meng
 *
 * 2016年4月20日 上午11:55:58
 */
public class AbstractRedisProtocolTest extends AbstractRedisTest{

	
	protected RedisClientProtocol<?> parse(RedisClientProtocol<?> parser, String[] contents) {
		
		ByteBuf []byteBufs = new ByteBuf[contents.length];
		
		for(int i = 0; i< contents.length;i++){
			
			byteBufs[i] = allocator.buffer();
			byteBufs[i].writeBytes(contents[i].getBytes());
		}

		RedisClientProtocol<?> result = null;
		
		for(ByteBuf byteBuf : byteBufs){
			result = parser.read(byteBuf);
		}
		
		return result;
	}

}
