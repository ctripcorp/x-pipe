package com.ctrip.xpipe.lifecycle;

import com.ctrip.xpipe.api.lifecycle.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author wenchao.meng
 *
 * Jun 6, 2016
 */
public class DefaultLifecycleState extends DefaultLifecycleController implements LifecycleState{
	
	private static final Logger logger = LoggerFactory.getLogger(DefaultLifecycleState.class);
	
	private AtomicReference<String> phaseName = new AtomicReference<>();

	private AtomicReference<String> previoisPhaseName = new AtomicReference<>();

	private Lifecycle lifecycle;
	
	private LifecycleController lifecycleController;
	
	public DefaultLifecycleState(Lifecycle lifecycle, LifecycleController lifecycleController) {
		this.lifecycle = lifecycle;
		this.lifecycleController = lifecycleController;
	}

	@Override
	public boolean isEmpty() {
		return phaseName.get() == null;
	}

	@Override
	public boolean isInitializing() {
		
		String phaseName = getPhaseName();
		return phaseName != null && phaseName.equals(Initializable.PHASE_NAME_BEGIN);
	}

	@Override
	public boolean isInitialized() {
		
		String phaseName = getPhaseName();
		return phaseName != null && phaseNameIn(phaseName, 
				Initializable.PHASE_NAME_END, 
				Startable.PHASE_NAME_BEGIN, 
				Startable.PHASE_NAME_END, 
				Stoppable.PHASE_NAME_BEGIN,
				Stoppable.PHASE_NAME_END);
	}

	@Override
	public boolean isStarting() {
		
		String phaseName = getPhaseName();
		return phaseName != null && phaseName.equals(Startable.PHASE_NAME_BEGIN);
	}

	@Override
	public boolean isStarted() {
		
		String phaseName = getPhaseName();
		return phaseName != null && phaseName.equals(Startable.PHASE_NAME_END);
	}

	@Override
	public boolean isStopping() {
		
		String phaseName = getPhaseName();
		return phaseName != null && phaseName.equals(Stoppable.PHASE_NAME_BEGIN);
	}

	@Override
	public boolean isStopped() {
		
		String phaseName = getPhaseName();
		return phaseName == null || 
					(	phaseNameIn(phaseName,
						Initializable.PHASE_NAME_END, 
						Stoppable.PHASE_NAME_END, 
						Disposable.PHASE_NAME_BEGIN, 
						Disposable.PHASE_NAME_END));
	}
	
	@Override
	public boolean isPositivelyStopped() {
		
		String phaseName = getPhaseName();
		return phaseName != null && phaseNameIn(phaseName, Stoppable.PHASE_NAME_END, Disposable.PHASE_NAME_BEGIN, Disposable.PHASE_NAME_END);
	}

	private boolean phaseNameIn(String phaseName, String ... ins) {
		
		for(String in : ins){
			if(phaseName.equals(in)){
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean isDisposing() {
		
		String phaseName = getPhaseName();
		return phaseName != null && phaseName.equals(Disposable.PHASE_NAME_BEGIN);
	}

	@Override
	public boolean isDisposed() {
		
		String phaseName = getPhaseName();
		return phaseName == null  || (phaseName.equals(Disposable.PHASE_NAME_END));
	}

	@Override
	public boolean isPositivelyDisposed() {
		
		String phaseName = getPhaseName();
		return phaseName != null && phaseNameIn(getPhaseName(), Disposable.PHASE_NAME_END);
	}

	@Override
	public String getPhaseName() {
		return phaseName.get();
	}

	@Override
	public void setPhaseName(String name) {
		
		logger.info("[setPhaseName]{}({}) --> {}", lifecycle, lifecycle.getClass().getSimpleName(), name);
		previoisPhaseName.set(phaseName.get());
		phaseName.set(name);
	}

	
	@Override
	public String toString() {
		return String.format("%s, %s", lifecycle.toString(), phaseName);
	}

	/**
	 * only support rollback once
	 */
	@Override
	public void rollback(Exception e) {
		
		logger.info("[rollback]{},{} -> {}, reason:{}", this, phaseName, previoisPhaseName, e.getMessage());
		phaseName.set(previoisPhaseName.get());
	}

	@Override
	public boolean canInitialize() {
		return lifecycleController.canInitialize(getPhaseName());
	}

	@Override
	public boolean canStart() {
		return lifecycleController.canStart(getPhaseName());
	}

	@Override
	public boolean canStop() {
		return lifecycleController.canStop(getPhaseName());
	}

	@Override
	public boolean canDispose() {
		return lifecycleController.canDispose(getPhaseName());
	}

}
