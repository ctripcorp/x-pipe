package com.ctrip.xpipe.redis.core.meta.comparator;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Triple;

import com.ctrip.xpipe.redis.core.meta.MetaComparator;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public abstract class AbstractMetaComparator<T> implements MetaComparator<T>{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	protected Set<T> added = new HashSet<>(); 
	protected Set<T> removed = new HashSet<>(); 
	@SuppressWarnings("rawtypes")
	protected Set<MetaComparator>  modified= new HashSet<>(); 
	

	
	/**
	 * @param current
	 * @param future
	 * @return added, modified, delted
	 */
	protected Triple<Set<String>, Set<String>, Set<String>> getDiff(Set<String> current, Set<String> future) {
		
		Set<String> added = new HashSet<>(future);
		Set<String> modified = new HashSet<>(future);
		Set<String> deleted = new HashSet<>(current);
		
		added.removeAll(deleted);
		modified.retainAll(deleted);
		deleted.removeAll(future);
		
		return new Triple<Set<String>, Set<String>, Set<String>>(added, modified, deleted);
	}

	@Override
	public Set<T> getAdded() {
		return added;
	}

	@Override
	public Set<T> getRemoved() {
		return removed;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Set<MetaComparator> getMofified() {
		return modified;
	}

}
