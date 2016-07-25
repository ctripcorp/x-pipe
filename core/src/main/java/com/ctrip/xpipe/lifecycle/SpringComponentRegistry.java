package com.ctrip.xpipe.lifecycle;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.springframework.context.ApplicationContext;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.TopElement;


/**
 * @author wenchao.meng
 *
 * Jun 17, 2016
 */
public class SpringComponentRegistry extends AbstractComponentRegistry{
	
	private ApplicationContext applicationContext;
	
	public SpringComponentRegistry(ApplicationContext applicationContext) {
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
		
		String []names = applicationContext.getBeanDefinitionNames();
		for(String name : names){
			
			Object bean = applicationContext.getBean(name);
			//only call topelement in spring
			if(bean instanceof Lifecycle && bean instanceof TopElement){
				result.add((Lifecycle)bean);
			}
		}
		
		Collections.sort(result, new Comparator<Lifecycle>() {

			@Override
			public int compare(Lifecycle o1, Lifecycle o2) {
				
				TopElement te1 = (TopElement) o1, te2 = (TopElement) o2;
				return (te1.getOrder() < te2.getOrder()) ? -1: (te1.getOrder() == te2.getOrder() ? 0: 1);
			}

		});
		return result;
	}

}
