package com.ctrip.xpipe.api.lifecycle;

/**
 * @author wenchao.meng
 *
 * Aug 11, 2016
 */
public interface Destroyable {
	
	void destroy() throws Exception;

}
