package com.ctrip.xpipe.api.lifecycle;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午4:57:05
 */
public interface Initializable {

	public static final String PHASE_NAME_BEGIN = "initializing";
	
	public static final String PHASE_NAME_END = "initialized";
	
	void initialize() throws Exception;
}
