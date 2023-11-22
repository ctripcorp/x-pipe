package com.ctrip.xpipe.lifecycle;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * @author wenchao.meng
 *
 * Jun 12, 2016
 */
public class CreatedComponentRedistry extends AbstractComponentRegistry implements ComponentRegistry{
	
	private Map<String, Object> components = new ConcurrentHashMap<>();
	
	private NameCreater nameCreator = new DefaultNameCreator();
	
	@Override
	public String doAdd(Object component) throws Exception {

		String name = getName(component);
		doAdd(name, component);
		return name;
	}

	private String getName(Object component) {
		return nameCreator.getName(component);
	}

	
	@Override
	protected Object doRemoveOfName(String name) {
		return components.remove(name);
	}
	
	@Override
	public boolean doRemove(Object component) throws Exception {
		
		String name = null;
		for(Entry<String, Object> entry : components.entrySet()){
			if(entry.getValue() == component){
				name = entry.getKey();
				break;
			}
		}
		
		if(name == null){
			logger.info("[doRemove][can not find component]{}", component);
			return false;
		}
		
		logger.info("[doRemove]{}, {}" , name, component);
		if(component instanceof Lifecycle){
			Lifecycle lifecycle = (Lifecycle) component;
			
			if(lifecycle.getLifecycleState() != null){
				if(lifecycle.getLifecycleState().canStop()){
					lifecycle.stop();
				}
				
				if(lifecycle.getLifecycleState().canDispose()){
					lifecycle.dispose();
				}
			}
		}

		components.remove(name);
		return true;
	}

	@Override
	public Object getComponent(String name) {
		return components.get(name);
	}


	@SuppressWarnings("unchecked")
	@Override
	protected <T> Map<String, T> doGetComponents(Class<T> clazz) {
		
		Map<String, T> result = new HashMap<>();
		for(Entry<String, Object> entry : components.entrySet()){
			if(clazz.isAssignableFrom(entry.getValue().getClass())){
				result.put(entry.getKey(), (T) entry.getValue());
			}
		}
		return result;
	}

	@Override
	protected void doAdd(String name, Object component) throws Exception {
	
		if(component instanceof Lifecycle){
			
			Lifecycle lifecycle = (Lifecycle) component;
			if(lifecycle.getLifecycleState() != null){
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
				
				if(getLifecycleState().isStopping() || getLifecycleState().isPositivelyStopped()){
					if(lifecycle.getLifecycleState().canStop()){
						lifecycle.stop();
					}
				}
				
				if(getLifecycleState().isDisposing() || getLifecycleState().isPositivelyDisposed()){
					if(lifecycle.getLifecycleState().canDispose()){
						lifecycle.dispose();
					}
				}

			}
		}
		
		components.put(name, component);
	}

	@Override
	public Map<String, Object> allComponents() {
		return new HashMap<>(components);
	}

	@Override
	public List<Lifecycle> lifecycleCallable() {
		
		List<Lifecycle> result = new LinkedList<>();
		
		for(Entry<String, Object> entry : components.entrySet()){
			if(entry.getValue() instanceof Lifecycle){
				result.add((Lifecycle)entry.getValue());
			}
		}
		return sort(result);
	}

	@Override
	public void cleanComponents() {
		components.clear();
	}
	
}
