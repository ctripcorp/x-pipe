package com.ctrip.xpipe.api.pool;

import com.ctrip.xpipe.pool.BorrowObjectException;
import com.ctrip.xpipe.pool.ReturnObjectException;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public interface SimpleKeyedObjectPool<K, V> {
	
	V borrowObject(K key) throws BorrowObjectException;
	
	void returnObject(K key, V value) throws ReturnObjectException;
	
	SimpleObjectPool<V> getKeyPool(K key);
	
	void clear() throws ObjectPoolException;
	
	void clear(K key) throws ObjectPoolException;

}
