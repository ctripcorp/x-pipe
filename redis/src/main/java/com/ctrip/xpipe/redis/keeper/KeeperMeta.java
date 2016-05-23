package com.ctrip.xpipe.redis.keeper;

/**
 * @author wenchao.meng
 *
 * May 23, 2016
 */
public interface KeeperMeta {
	
	String getKeeperName();

	int getKeeperPort();
	
	String getKeeperRunid();
	
}
