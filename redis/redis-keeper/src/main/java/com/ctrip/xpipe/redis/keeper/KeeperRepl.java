package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.ReplStage;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * May 23, 2016
 */
public interface KeeperRepl {

	ReplStage preStage();

	ReplStage currentStage();

	GtidSet getGtidSetExecuted();

	GtidSet getGtidSetLost();

	String replId();
	
	String replId2();
	
	Long   secondReplIdOffset();

	long backlogBeginOffset();

	long getBeginOffset();
	
	long getEndOffset();

	GtidSet getBeginGtidSet() throws IOException;

	GtidSet getEndGtidSet() throws IOException;

	boolean supportGtidSet();

}
