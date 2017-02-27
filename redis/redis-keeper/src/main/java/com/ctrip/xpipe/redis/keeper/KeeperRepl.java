package com.ctrip.xpipe.redis.keeper;

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
}
