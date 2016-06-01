package com.ctrip.xpipe.api.lifecycle;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午4:57:05
 */
public interface Stoppable {
	
	public static final String PHASE_NAME_BEGIN = "stopping";
	
	public static final String PHASE_NAME_END = "stopped";

	void stop() throws Exception;
}
