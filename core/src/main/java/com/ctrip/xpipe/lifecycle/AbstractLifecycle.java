package com.ctrip.xpipe.lifecycle;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.ctrip.xpipe.api.lifecycle.Disposable;
import com.ctrip.xpipe.api.lifecycle.Initializable;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.LifecycleController;
import com.ctrip.xpipe.api.lifecycle.LifecycleState;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午4:59:41
 */
public abstract class AbstractLifecycle implements Lifecycle{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private LifecycleState lifecycleState;
	private LifecycleController LifecycleController;
	
	public AbstractLifecycle() {
		this.lifecycleState = new DefaultLifecycleState(this);
		this.LifecycleController  = new DefaultLifecycleController();
	}
	
	
	
	@Override
	public void initialize() throws Exception {
		try{
			
			String phaseName = lifecycleState.getPhaseName();
			if(!LifecycleController.canInitialize(phaseName)){
				logger.error("[initialize][can not init]" + this);
				throw new IllegalStateException("can not initialize" + this);
			}
			

			lifecycleState.setPhaseName(Initializable.PHASE_NAME_BEGIN);
			doInitialize();
		}finally{
			lifecycleState.setPhaseName(Initializable.PHASE_NAME_END);
		}
	}
	
	protected void doInitialize() throws Exception{
		
	}

	@Override
	public void start() throws Exception {
		try{
			String phaseName = lifecycleState.getPhaseName();
			if(!LifecycleController.canStart(phaseName)){
				logger.error("[initialize][can not start]" + this);
				throw new IllegalStateException("can not start" + this);
			}
			lifecycleState.setPhaseName(Startable.PHASE_NAME_BEGIN);

			doStart();
		}finally{
			lifecycleState.setPhaseName(Startable.PHASE_NAME_END);
		}
	}
	
	protected void doStart() throws Exception{
		
	}

	@Override
	public void stop() throws Exception {
		try{
			String phaseName = lifecycleState.getPhaseName();
			if(!LifecycleController.canStop(phaseName)){
				logger.error("[initialize][can not stop]" + this);
				throw new IllegalStateException("can not stop" + this);
			}
			
			lifecycleState.setPhaseName(Stoppable.PHASE_NAME_BEGIN);
			doStop();
		}finally{
			lifecycleState.setPhaseName(Stoppable.PHASE_NAME_BEGIN);
		}
	}

	protected void doStop() throws Exception{
		
	}

	@Override
	public void dispose() throws Exception {
		try{
			
			String phaseName = lifecycleState.getPhaseName();
			if(!LifecycleController.canDispose(phaseName)){
				logger.error("[initialize][can not dispose]" + this);
				throw new IllegalStateException("can not dispose" + this);
			}
			lifecycleState.setPhaseName(Disposable.PHASE_NAME_BEGIN);
			doDispose();
		}finally{
			lifecycleState.setPhaseName(Disposable.PHASE_NAME_BEGIN);
		}
	}

	protected void doDispose() throws Exception {
		
	}

	public LifecycleState getLifecycleState(){
		
		return this.lifecycleState;
	}
	
	@Override
	public String toString() {
		
		return getClass().getSimpleName() + ", phase:" + lifecycleState.getPhaseName();
	}
}
