package com.ctrip.xpipe.api.lifecycle;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午4:57:05
 */
public interface Disposable {
	
	public static final String PHASE_NAME_BEGIN = "disposing";
	
	public static final String PHASE_NAME_END = "disposed";

	void dispose() throws Exception;
}
