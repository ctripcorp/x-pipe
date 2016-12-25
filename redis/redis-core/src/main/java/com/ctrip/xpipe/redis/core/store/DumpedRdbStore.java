package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;

/**
 * @author wenchao.meng
 *
 * Aug 26, 2016
 */
public interface DumpedRdbStore extends RdbStore{
	
	void setEofType(EofType eofType);
	
	void setMasterOffset(long masterOffset);
	
	EofType getEofType();
	
	long getMasterOffset();

	void setRdbLastKeeperOffset(long rdbLastKeeperOffset);

}
