package com.ctrip.xpipe.pool;

import com.ctrip.xpipe.api.pool.ObjectPoolException;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class XpipeObjectPoolFromKeyed<K, V> implements SimpleObjectPool<V>{
	
	
	private SimpleKeyedObjectPool<K, V> keyedObjectPool;
	
	private K key;
	
	public XpipeObjectPoolFromKeyed(SimpleKeyedObjectPool<K, V> keyedObjectPool, K key) {
		this.key = key;
		this.keyedObjectPool = keyedObjectPool;
	}

	@Override
	public V borrowObject() throws BorrowObjectException {
		
		return keyedObjectPool.borrowObject(key);
	}
	
	@Override
	public void returnObject(V obj) throws ReturnObjectException{
		
		keyedObjectPool.returnObject(key, obj);
	}

	@Override
	public void clear() throws ObjectPoolException {
	}

	@Override
	public String desc() {
		return String.format("Key:%s", key.toString());
	}
}
