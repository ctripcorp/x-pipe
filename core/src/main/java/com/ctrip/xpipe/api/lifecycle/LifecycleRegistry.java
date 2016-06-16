package com.ctrip.xpipe.api.lifecycle;

import java.util.List;

/**
 * lifecycle component container
 * @author wenchao.meng
 *
 * Jun 12, 2016
 */
public interface LifecycleRegistry extends Lifecycle{
	
	
	void add(Lifecycle lifecycle) throws Exception;
	
	void remove(Lifecycle lifecycle) throws Exception;
	
	List<Lifecycle>  getLifecycles(Class<?> clazz);

}
