package com.ctrip.xpipe.lifecycle;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.lifecycle.Disposable;
import com.ctrip.xpipe.api.lifecycle.Initializable;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.LifecycleState;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

/**
 * @author wenchao.meng
 *
 * Jun 6, 2016
 */
public class DefaultLifecycleState implements LifecycleState{
	
	private static final Logger logger = LoggerFactory.getLogger(DefaultLifecycleState.class);
	
	private AtomicReference<String> phaseName = new AtomicReference<>();

	private AtomicReference<String> previoisPhaseName = new AtomicReference<>();

	private Lifecycle lifecycle;
	
	public DefaultLifecycleState(Lifecycle lifecycle) {
		this.lifecycle = lifecycle;
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
				(phaseName != null && phaseNameIn(phaseName, 
						Initializable.PHASE_NAME_END, 
						Stoppable.PHASE_NAME_END, 
						Disposable.PHASE_NAME_BEGIN, 
						Disposable.PHASE_NAME_END));
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
		return phaseName == null  || (phaseName != null && phaseName.equals(Disposable.PHASE_NAME_END));
	}

	@Override
	public String getPhaseName() {
		return phaseName.get();
	}

	@Override
	public void setPhaseName(String name) {
		
		logger.info("[setPhaseName]{} --> {}", lifecycle, name);
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

}
