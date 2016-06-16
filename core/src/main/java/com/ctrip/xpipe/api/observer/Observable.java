package com.ctrip.xpipe.api.observer;

/**
 * @author wenchao.meng
 *
 * May 18, 2016 4:12:53 PM
 */
public interface Observable {
	
	void addObserver(Observer observer);
	
	void removeObserver(Observer observer);
	
	
}
