package com.ctrip.xpipe.lifecycle;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.exception.LifecycleLogPassRuntimeException;
import com.ctrip.xpipe.exception.LifecycleLogpassException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author wenchao.meng
 *
 * Jun 17, 2016
 */
public abstract class AbstractComponentRegistry extends AbstractLifecycle implements ComponentRegistry{
	
	@Override
	public String add(Object component) throws Exception {
		
		return doAdd(component);
	}
	
	@Override
	public void add(String name, Object component) throws Exception {
		doAdd(name, component);
	}
	
	protected abstract void doAdd(String name, Object component) throws Exception;

	protected abstract String doAdd(Object component) throws Exception;

	@Override
	public boolean remove(Object component) throws Exception {
		
		return doRemove(component);
	}
	
	@Override
	public Object removeOfName(String name) {
		
		return doRemoveOfName(name);
	}
	
	protected abstract Object doRemoveOfName(String name);

	protected abstract boolean doRemove(Object component) throws Exception;

	@Override
	public <T> Map<String, T>  getComponents(Class<T> clazz) {
		
		return doGetComponents(clazz);
	}
	
	protected abstract <T> Map<String, T>  doGetComponents(Class<T> clazz);

	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		
		for(Lifecycle lifecycle : lifecycleCallable()){
			if(lifecycle.getLifecycleState().canInitialize()){
				try{
					lifecycle.initialize();
				}catch(Throwable th){					
					if(!logpass(th)){
						throw th;
					}
					logger.error("[doInitialize]" + lifecycle, th);
				}
			}
		}
	}
	
	private boolean logpass(Throwable e) {
		return e instanceof LifecycleLogpassException || e instanceof LifecycleLogPassRuntimeException;
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();
		
		for(Lifecycle lifecycle : lifecycleCallable()){
			if(lifecycle.getLifecycleState().canStart()){
				try{
					lifecycle.start();
				}catch(Throwable th){
					if(!logpass(th)){
						throw th;
					}
					logger.error("[doStart]" + lifecycle, th);
				}
			}
		}
	}

	@Override
	public void destroy() throws Exception {
		
		logger.info("[destroy]");
		stopAllComponents();
		disposeAllComponents();
		cleanComponents();
	}

	private void disposeAllComponents() {
		List<Lifecycle> components = lifecycleCallable();
		Collections.reverse(components);
		
		for(Lifecycle lifecycle : components){
			if(lifecycle.getLifecycleState().canDispose()){
				try{
					lifecycle.dispose();
				}catch(Throwable th){
					logger.error("[doDispose]" + lifecycle, th);
				}
			}
		}
	}

	private void stopAllComponents() {
		
		List<Lifecycle> components = lifecycleCallable();
		Collections.reverse(components);
		
		for(Lifecycle lifecycle : components){
				if(lifecycle.getLifecycleState().canStop()){
					try{
						lifecycle.stop();
					}catch(Throwable th){
						logger.error("[doStop]" + lifecycle, th);
					}
			}
		}
	}

	@Override
	protected void doStop() throws Exception {

		stopAllComponents();
		super.doStop();
		
	}
	
	@Override
	protected void doDispose() throws Exception {
		disposeAllComponents();
		super.doDispose();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> T getComponent(Class<T> clazz) {
		
		Map<String, T> all = getComponents(clazz);
		if(all.size() == 0){
			throw new IllegalStateException("no component of type:" + clazz);
		}
		
		if(all.size() > 1){
			throw new IllegalStateException("component of type more than one:" + clazz + "," + all.size());
		}
		return (T) all.values().toArray()[0];
	}

	
	protected List<Lifecycle> sort(List<Lifecycle> result) {

		Collections.sort(result, new OrderedComparator());
		return result;
	}

}
