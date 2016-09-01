package com.ctrip.xpipe.api.lifecycle;

/**
 * @author wenchao.meng
 *
 * Jun 6, 2016
 */
public interface LifecycleState{
	
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
	
	boolean isPositivelyStopped();
	
	boolean isDisposing();
	
	boolean isDisposed();

	boolean isPositivelyDisposed();

	String getPhaseName();
	
	void setPhaseName(String name);
	
	/**
	 * rollback to previous state
	 */
	void rollback(Exception e);

	boolean canInitialize();

	boolean canStart();

	boolean canStop();

	boolean canDispose();

}
