package com.ctrip.xpipe.redis.keeper;

/**
 * @author wenchao.meng
 *
 * Mar 20, 2017
 */
public interface Keeperable {
	
	boolean isKeeper();
	
	void setKeeper();

	default String roleDesc(){
		if(isKeeper()){
			return "keeper";
		}
		return "redis";
	}

}
