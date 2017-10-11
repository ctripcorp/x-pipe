package com.ctrip.xpipe.redis.core.protocal.protocal;


import com.ctrip.xpipe.redis.core.protocal.CAPA;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;

import java.io.File;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Dec 23, 2016
 */
public class LenEofType extends AbstractEofType{
	
	private final long expectedLen;
	
	public LenEofType(long expectedLen) {
		this.expectedLen = expectedLen;
	}

	@Override
	public boolean putOnLineOnAck() {
		return false;
	}

	@Override
	public String getTag() {
		return String.valueOf(expectedLen);
	}

	@Override
	public ByteBuf getStart() {
		
    	RequestStringParser parser = new RequestStringParser(String.valueOf((char)RedisClientProtocol.DOLLAR_BYTE) 
    			+String.valueOf(expectedLen)); 
		return parser.format();
	}

	@Override
	public ByteBuf getEnd() {
		return null;
	}

	@Override
	public boolean fileOk(File file) {
		
		if(expectedLen == file.length()){
			return true;
		}
		return false;
	}
	
	@Override
	public String toString() {
		return String.format("expectedLen:%d", expectedLen);
	}

	@Override
	public boolean support(Set<CAPA> capas) {
		return true;
	}
}
