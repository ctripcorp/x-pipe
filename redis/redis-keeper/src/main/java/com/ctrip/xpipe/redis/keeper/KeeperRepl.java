package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.gtid.GtidSet;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * May 23, 2016
 */
public interface KeeperRepl {
	
	String replId();
	
	String replId2();
	
	Long   secondReplIdOffset();
	
	long getBeginOffset();
	
	long getEndOffset();

	GtidSet getBeginGtidSet() throws IOException;

	GtidSet getEndGtidSet();

}
