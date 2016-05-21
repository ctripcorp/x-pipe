package com.ctrip.xpipe.api.monitor;

/**
 * @author wenchao.meng
 *
 * May 21, 2016 10:13:30 PM
 */
public interface DelayMonitor {

	
	void addData(long lastTime);
	
	String getDelayType();
	
}
