package com.ctrip.xpipe.lifecycle;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.utils.MapUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *
 * Jun 17, 2016
 */
public class DefaultNameCreator implements NameCreater{
	
	private Map<Class<?>,  AtomicInteger>  names = new ConcurrentHashMap<>();
	
	@Override
	public String getName(Object component) {
		
		AtomicInteger count = MapUtils.getOrCreate(names, component.getClass(), new ObjectFactory<AtomicInteger>() {

			@Override
			public AtomicInteger create() {
				return new AtomicInteger();
			}
		});
		return component.getClass().getSimpleName() + "-" + count.incrementAndGet();
	}

}
