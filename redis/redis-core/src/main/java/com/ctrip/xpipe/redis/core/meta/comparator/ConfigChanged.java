package com.ctrip.xpipe.redis.core.meta.comparator;

/**
 * @author wenchao.meng
 *
 * Sep 2, 2016
 */
public interface ConfigChanged<T extends Enum<T>> {
	
	T getChangedType();
}
