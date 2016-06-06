package com.ctrip.xpipe.api.lifecycle;

/**
 * @author wenchao.meng
 *
 * Jun 6, 2016
 */
public interface LifecycleController {
	
	boolean canInitialize(String phaseName);
	
	boolean canStart(String phaseName);
	
	boolean canStop(String phaseName);
	
	boolean canDispose(String phaseName);
}
