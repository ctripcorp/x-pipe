package com.ctrip.xpipe.redis.core.meta;

/**
 * @author wenchao.meng
 *
 * Sep 4, 2016
 */
public interface MetaComparatorVisitor<T> {
	
	void visitAdded(T added);

	void visitModified(@SuppressWarnings("rawtypes") MetaComparator comparator);
	
	void visitRemoved(T removed);
}
