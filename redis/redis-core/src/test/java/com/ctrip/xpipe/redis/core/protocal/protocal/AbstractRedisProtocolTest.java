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


	private long totalReadLen = 0;

	protected RedisClientProtocol<?> parse(RedisClientProtocol<?> parser, String[] contents) {

		totalReadLen = 0;

		ByteBuf []byteBufs = new ByteBuf[contents.length];

		for(int i = 0; i< contents.length;i++){

			byteBufs[i] = directByteBuf();
			byteBufs[i].writeBytes(contents[i].getBytes());
		}

		RedisClientProtocol<?> result = null;

		for(ByteBuf byteBuf : byteBufs){

			int before = byteBuf.readableBytes();
			result = parser.read(byteBuf);
			int after = byteBuf.readableBytes();
			totalReadLen += before - after;
			if(result != null){
				break;
			}
		}
		return result;
	}

	public long getTotalReadLen() {
		return totalReadLen;
	}

}
