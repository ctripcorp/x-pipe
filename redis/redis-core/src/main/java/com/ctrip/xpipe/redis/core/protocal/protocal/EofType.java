package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.redis.core.protocal.CAPA;
import io.netty.buffer.ByteBuf;

import java.io.File;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Dec 23, 2016
 */
public interface EofType {
	
	boolean putOnLineOnAck();
	
	boolean fileOk(File file);
	
	String getTag();

	ByteBuf getStart();
	
	ByteBuf getEnd();

	boolean support(Set<CAPA> capas);
	
}
