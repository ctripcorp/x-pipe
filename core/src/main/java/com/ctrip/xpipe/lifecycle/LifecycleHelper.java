package com.ctrip.xpipe.lifecycle;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public class LifecycleHelper {
	
	public static void initializeIfPossible(Object obj) throws Exception{
		
		if(obj instanceof Lifecycle && ((Lifecycle) obj).getLifecycleState().canInitialize()){
			((Lifecycle) obj).initialize();
		}
		
	}

	
	public static void startIfPossible(Object obj) throws Exception{
		
		if(obj instanceof Lifecycle && ((Lifecycle) obj).getLifecycleState().canStart()){
			((Lifecycle) obj).start();
		}
		
	}

	public static void stopIfPossible(Object obj) throws Exception{
		
		if(obj instanceof Lifecycle && ((Lifecycle) obj).getLifecycleState().canStop()){
			((Lifecycle) obj).stop();
		}
	}
	
	public static void disposeIfPossible(Object obj) throws Exception{
		if(obj instanceof Lifecycle && ((Lifecycle) obj).getLifecycleState().canDispose()){
			((Lifecycle) obj).dispose();
		}
	}

}
