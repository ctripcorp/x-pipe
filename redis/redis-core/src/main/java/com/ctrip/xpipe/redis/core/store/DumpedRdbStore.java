package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;

import java.io.File;

/**
 * @author wenchao.meng
 *
 * Aug 26, 2016
 */
public interface DumpedRdbStore extends RdbStore{
	
	void setEofType(EofType eofType);
		
	EofType getEofType();
	
	void setRdbOffset(long rdbLastOffset);
	
	File getRdbFile();

}
