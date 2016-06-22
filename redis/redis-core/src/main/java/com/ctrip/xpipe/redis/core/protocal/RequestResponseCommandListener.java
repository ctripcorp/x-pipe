package com.ctrip.xpipe.redis.core.protocal;


/**
 * @author wenchao.meng
 *
 * May 11, 2016 6:13:34 PM
 */
public interface RequestResponseCommandListener {
	
	/**
	 * @param data 
	 * @param e  if e == null, means success, else failure
	 */
	void onComplete(CmdContext cmdContext, Object data, Exception e);

}
