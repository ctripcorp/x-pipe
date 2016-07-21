package com.ctrip.xpipe.api.cluster;

/**
 * @author wenchao.meng
 *
 * Jul 21, 2016
 */
public interface LeaderAware {
	
	void isleader();
	
	void notLeader();

}
