package com.ctrip.xpipe.lifecycle;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;


/**
 * @author wenchao.meng
 *
 * Jun 17, 2016
 */
public class SpringComponentRegistry extends AbstractComponentRegistry implements ApplicationContextAware{
	
	private ApplicationContext applicationContext;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
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

}
