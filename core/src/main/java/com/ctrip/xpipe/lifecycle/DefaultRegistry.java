package com.ctrip.xpipe.lifecycle;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author wenchao.meng
 *
 * Jun 17, 2016
 */
public class DefaultRegistry extends AbstractComponentRegistry{
	
	private ComponentRegistry createdRegistry; 
	private ComponentRegistry springRegistry;
	
	public DefaultRegistry(ComponentRegistry createdRegistry, ComponentRegistry springRegistry) {
		
		this.createdRegistry = createdRegistry;
		this.springRegistry = springRegistry;
	}

	@Override
	public Object getComponent(String name) {
		
		Object component = null;
		
		if(springRegistry != null){
			component = springRegistry.getComponent(name);
		}
		
		if(component == null){
			component = createdRegistry.getComponent(name);
		}
		return component;
	}

	@Override
	protected void doAdd(String name, Object component) throws Exception {
		createdRegistry.add(name, component);
	}

	@Override
	protected String doAdd(Object component) throws Exception {
		
		return createdRegistry.add(component);
	}

	@Override
	protected Object doRemoveOfName(String name) {
		return createdRegistry.removeOfName(name);
	}

	@Override
	protected boolean doRemove(Object component) throws Exception {
		return createdRegistry.remove(component);
	}

	@Override
	protected <T> Map<String, T> doGetComponents(Class<T> clazz) {
		
		Map<String, T> result = new HashMap<>(createdRegistry.getComponents(clazz));
		if(springRegistry != null){
			result.putAll(springRegistry.getComponents(clazz));
		}
		return result;
	}

	@Override
	public Map<String, Object> allComponents() {
		
		Map<String, Object> result = new HashMap<>(createdRegistry.allComponents());
		if(springRegistry != null){
			result.putAll(springRegistry.allComponents());
		}
		return result;
	}
	
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		
		createdRegistry.initialize();
		if(springRegistry != null){
			springRegistry.initialize();
		}
	}
	
	@Override
	protected void doStart() throws Exception {
		super.doStart();
		
		createdRegistry.start();
		if(springRegistry != null){
			springRegistry.start();
		}
	}
	
	@Override
	protected void doStop() throws Exception {
		super.doStop();
		
		createdRegistry.stop();
		if(springRegistry != null){
			springRegistry.stop();
		}
	}

	@Override
	protected void doDispose() throws Exception {
		super.doDispose();
		
		createdRegistry.dispose();
		if(springRegistry != null){
			springRegistry.dispose();
		}
	}
	@Override
	public List<Lifecycle> lifecycleCallable() {
		
		List<Lifecycle> result = new LinkedList<>();
		result.addAll(createdRegistry.lifecycleCallable());
		if(springRegistry != null){
			result.addAll(springRegistry.lifecycleCallable());
		}
		return sort(result);
	}
	
	@Override
	public void cleanComponents(){
		
		createdRegistry.cleanComponents();
		if(springRegistry != null){
			springRegistry.cleanComponents();
		}
	}
}
