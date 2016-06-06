package com.ctrip.xpipe.api.lifecycle;

/**
 * @author wenchao.meng
 *
 * Jun 6, 2016
 */
public interface LifecycleState {
	
	/**
	 * just created!
	 * @return
	 */
	boolean isEmpty();

	boolean isInitializing();
	
	boolean isInitialized();
	
	boolean isStarting();
	
	boolean isStarted();
	
	boolean isStopping();
	
	boolean isStopped();
	
	boolean isDisposing();
	
	boolean isDisposed();
	
	String getPhaseName();
	
	void setPhaseName(String name);
	
}
