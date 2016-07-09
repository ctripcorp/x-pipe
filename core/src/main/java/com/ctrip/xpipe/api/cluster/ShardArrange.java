package com.ctrip.xpipe.api.cluster;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public interface ShardArrange<K> {
	
	boolean responsableFor(K key);

}
