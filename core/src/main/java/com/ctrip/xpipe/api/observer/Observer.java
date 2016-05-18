package com.ctrip.xpipe.api.observer;

/**
 * @author wenchao.meng
 *
 * May 18, 2016 4:13:30 PM
 */
public interface Observer {
	
	void update(Object args, Observable observable);

}
