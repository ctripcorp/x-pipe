package com.ctrip.xpipe.api.monitor;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

/**
 * @author wenchao.meng
 *
 * May 21, 2016 10:13:30 PM
 */
public interface DelayMonitor extends Startable, Stoppable{

	
	void addData(long lastTime);
	
	String getDelayType();
	
	void setDelayInfo(String info);

	void setConsolePrint(boolean consolePrint);

}
