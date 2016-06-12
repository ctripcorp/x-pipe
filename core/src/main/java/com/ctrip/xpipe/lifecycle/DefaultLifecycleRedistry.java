package com.ctrip.xpipe.lifecycle;

import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.LifecycleRegistry;

/**
 * 
 * @author wenchao.meng
 *
 * Jun 12, 2016
 */
public class DefaultLifecycleRedistry extends AbstractLifecycle implements LifecycleRegistry{
	
	private List<Lifecycle> components = new LinkedList<>();
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		
		for(Lifecycle lifecycle : components){
			if(lifecycle.getLifecycleState().canInitialize()){
				lifecycle.initialize();
			}
		}
	}
	
	@Override
	protected void doStart() throws Exception {
		super.doStart();
		
		for(Lifecycle lifecycle : components){
			if(lifecycle.getLifecycleState().canStart()){
				lifecycle.start();
			}
		}
	}

	
	@Override
	protected void doStop() throws Exception {
		
		for(Lifecycle lifecycle : components){
			if(lifecycle.getLifecycleState().canStop()){
				lifecycle.stop();
			}
		}
		super.doStop();
		
	}
	
	@Override
	protected void doDispose() throws Exception {

		for(Lifecycle lifecycle : components){
			if(lifecycle.getLifecycleState().canDispose()){
				lifecycle.dispose();
			}
		}
		super.doDispose();
	}

	
	@Override
	
	public void add(Lifecycle lifecycle) throws Exception {
		
		if(getLifecycleState().isInitializing() || getLifecycleState().isInitialized()){
			if(lifecycle.getLifecycleState().canInitialize()){
				lifecycle.initialize();
			}
		}
		
		if(getLifecycleState().isStarting() || getLifecycleState().isStarted()){
			if(lifecycle.getLifecycleState().canStart()){
				lifecycle.start();
			}
		}
		
		components.add(lifecycle);
	}

	@Override
	public void remove(Lifecycle lifecycle) throws Exception {
		
		boolean remove = components.remove(lifecycle);
		if(!remove){
			return;
		}
		
		if(lifecycle.getLifecycleState().canStop()){
			lifecycle.stop();
		}
		
		if(lifecycle.getLifecycleState().canDispose()){
			lifecycle.dispose();
		}
	}
}
