package com.ctrip.xpipe.api.lifecycle;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午4:57:05
 */
public interface Startable {
	public static final String PHASE_NAME_BEGIN = "starting";

	public static final String PHASE_NAME_END = "started";

	void start() throws Exception;

}
