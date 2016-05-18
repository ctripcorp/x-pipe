package com.ctrip.xpipe.api.observer;

/**
 * @author wenchao.meng
 *
 * May 18, 2016 4:12:53 PM
 */
public interface Observable {
	
	void addOObserver(Observer observer);
	
	void remoteObserver(Observer observer);
	
	
}
