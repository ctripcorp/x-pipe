package com.ctrip.xpipe.redis.core.protocal.protocal;


import com.ctrip.xpipe.redis.core.protocal.CAPA;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Dec 23, 2016
 */
public class EofMarkType extends AbstractEofType{
	
	private String eofMark;
	public EofMarkType(String eofMark) {
		this.eofMark = eofMark;
	}

	@Override
	public boolean putOnLineOnAck() {
		return true;
	}

	@Override
	public String getTag() {
		return eofMark;
	}

	@Override
	public ByteBuf getStart() {
		
    	RequestStringParser parser = new RequestStringParser(
    			String.valueOf((char)RedisClientProtocol.DOLLAR_BYTE) 
    			+ new String(RedisClientProtocol.EOF)  
    			+ eofMark); 
		return parser.format();
	}

	@Override
	public ByteBuf getEnd() {
		return Unpooled.wrappedBuffer(eofMark.getBytes());
	}

	@Override
	public boolean fileOk(File file) {
		return true;
	}
	
	@Override
	public String toString() {
		
		return String.format("eofmark:%s", eofMark);
	}

	@Override
	public boolean support(Set<CAPA> capas) {
		return capas.contains(CAPA.EOF);
	}

}
