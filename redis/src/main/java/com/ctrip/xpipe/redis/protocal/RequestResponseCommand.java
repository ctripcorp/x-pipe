package com.ctrip.xpipe.redis.protocal;

/**
 * send request 
 * receive response
 * @author wenchao.meng
 *
 * May 11, 2016 5:48:20 PM
 */
public interface RequestResponseCommand extends Command{
	
	void setCommandListener(RequestResponseCommandListener commandListener);

}
