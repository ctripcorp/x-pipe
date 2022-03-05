package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.redis.core.meta.comparator.ConfigChanged;

import java.util.List;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public interface MetaComparator<T, C extends Enum<C>> {
	
	Set<T> getAdded();
	
	Set<T> getRemoved();
	
	@SuppressWarnings("rawtypes")
	Set<MetaComparator> getMofified();
	
	void compare();

	List<ConfigChanged<C>> getConfigChanged();
	
	/**
	 * add or remvoed or removed
	 * @return
	 */
	int totalChangedCount();

	boolean isShallowChange();

	String idDesc();
	
	void accept(MetaComparatorVisitor<T> visitor);
}
