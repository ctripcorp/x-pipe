package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;

/**
 * @author wenchao.meng
 *
 * Aug 26, 2016
 */
public interface DumpedRdbStore extends RdbStore{

	void setReplId(String replId);
	
	void setEofType(EofType eofType);

	void setRdbOffset(long rdbLastOffset);

}
