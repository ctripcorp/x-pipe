package com.ctrip.xpipe.redis.core.store;

/**
 * @author wenchao.meng
 *
 * Aug 26, 2016
 */
public interface DumpedRdbStore extends RdbStore{
	
	void setRdbFileSize(long rdbFileSize);
	
	void setMasterOffset(long masterOffset);
	
	long getRdbFileSize();
	
	long getMasterOffset();

	void setRdbLastKeeperOffset(long rdbLastKeeperOffset);

}
