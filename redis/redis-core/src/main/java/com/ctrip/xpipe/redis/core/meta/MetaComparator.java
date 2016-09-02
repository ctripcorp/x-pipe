package com.ctrip.xpipe.redis.core.meta;

import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public interface MetaComparator<T> {
	
	Set<T> getAdded();
	
	Set<T> getRemoved();
	
	@SuppressWarnings("rawtypes")
	Set<MetaComparator> getMofified();
	
	void compare();

}
