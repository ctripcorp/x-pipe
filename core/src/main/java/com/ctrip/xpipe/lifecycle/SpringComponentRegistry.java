package com.ctrip.xpipe.lifecycle;


import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author wenchao.meng
 *
 * Jun 17, 2016
 */
public class SpringComponentRegistry extends AbstractComponentRegistry{
	
	private ConfigurableApplicationContext applicationContext;
	
	public SpringComponentRegistry(ConfigurableApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}


	@Override
	public Object getComponent(String name) {
		return applicationContext.getBean(name);
	}

	@Override
	protected void doAdd(String name, Object component) throws Exception {
		throw new IllegalStateException("unsupported add operation...");
	}

	@Override
	protected String doAdd(Object component) throws Exception {
		throw new IllegalStateException("unsupported add operation...");
	}

	@Override
	protected Object doRemoveOfName(String name) {
		throw new IllegalStateException("unsupported remove operation...");
	}

	@Override
	protected boolean doRemove(Object component) throws Exception {
		throw new IllegalStateException("unsupported remove operation...");
	}

	@Override
	protected <T> Map<String, T> doGetComponents(Class<T> clazz) {
		return applicationContext.getBeansOfType(clazz);
	}

	@Override
	public Map<String, Object> allComponents() {
		
		Map<String, Object> result = new HashMap<>();
		String []names = applicationContext.getBeanDefinitionNames();
		for(String name : names){
			result.put(name, applicationContext.getBean(name));
		}
		return result;
	}

	@Override
	public List<Lifecycle> lifecycleCallable() {
		
		List<Lifecycle> result = new LinkedList<>();
		
		Map<String, Lifecycle> beans = applicationContext.getBeansOfType(Lifecycle.class);
		for(Entry<String, Lifecycle> entry : beans.entrySet()){
			
			@SuppressWarnings("unused")
			String name = entry.getKey();
			Lifecycle bean = entry.getValue();
			if(bean instanceof TopElement){
				result.add((Lifecycle)bean);
			}
		}
		
		return sort(result);
	}

	@Override
	public void cleanComponents() {
		//nothing to do
		applicationContext.close();
	}
}
