package com.ctrip.xpipe.api.lifecycle;

import java.util.List;
import java.util.Map;

/**
 * component container
 * @author wenchao.meng
 *
 * Jun 12, 2016
 */
public interface ComponentRegistry extends Lifecycle, Destroyable{
	
	String add(Object component) throws Exception;
	
	void add(String name, Object component) throws Exception;
	
	boolean remove(Object component) throws Exception;
	
	Object removeOfName(String name);

	<T> Map<String, T>  getComponents(Class<T> clazz);
	
	<T>  T getComponent(Class<T> clazz);
	
	Object getComponent(String name);
	
	Map<String, Object> allComponents();
	
	List<Lifecycle> lifecycleCallable();
	
	void cleanComponents();	
}
