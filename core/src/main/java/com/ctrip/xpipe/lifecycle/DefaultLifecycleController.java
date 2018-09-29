package com.ctrip.xpipe.lifecycle;

import com.ctrip.xpipe.api.lifecycle.*;

/**
 * @author wenchao.meng
 *
 * Jun 6, 2016
 */
public class DefaultLifecycleController implements LifecycleController{

	@Override
	public boolean canInitialize(String phaseName) {
		
		return phaseName == null || phaseName.equals(Disposable.PHASE_NAME_END);
	}

	@Override
	public boolean canStart(String phaseName) {
		
		return phaseName != null  && 
				(phaseName.equals(Initializable.PHASE_NAME_END) ||
				 phaseName.equals(Stoppable.PHASE_NAME_END));
	}

	@Override
	public boolean canStop(String phaseName) {
		
		return phaseName != null  && 
				(phaseName.equals(Startable.PHASE_NAME_END));
	}

	@Override
	public boolean canDispose(String phaseName) {
		
		return phaseName != null  && 
				(phaseName.equals(Initializable.PHASE_NAME_END) || 
				phaseName.equals(Stoppable.PHASE_NAME_END)
				);
	}
	
}
