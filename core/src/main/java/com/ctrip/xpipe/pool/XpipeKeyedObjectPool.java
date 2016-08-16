package com.ctrip.xpipe.pool;


import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.springframework.core.Ordered;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

/**
 * let pool implements lifecycle
 * @author wenchao.meng
 *
 * Jun 28, 2016
 */
public class XpipeKeyedObjectPool<K, V> extends AbstractLifecycle implements TopElement, SimpleKeyedObjectPool<K, V>{
	
	private KeyedObjectPool<K, V>  objectPool;
	private KeyedPooledObjectFactory<K, V> pooledObjectFactory;
	private GenericKeyedObjectPoolConfig config;

	public XpipeKeyedObjectPool(KeyedPooledObjectFactory<K, V> pooledObjectFactory){
		this(pooledObjectFactory, createDefaultConfig());
	}

	
	private static GenericKeyedObjectPoolConfig createDefaultConfig() {
		GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
		config.setBlockWhenExhausted(false);
		return config;
	}

	public XpipeKeyedObjectPool(KeyedPooledObjectFactory<K, V> pooledObjectFactory, GenericKeyedObjectPoolConfig config){
		this.pooledObjectFactory = pooledObjectFactory;
		this.config = config;
		
	}
	
	@Override
	protected void doInitialize() throws Exception {
		GenericKeyedObjectPool<K, V> genericKeyedObjectPool = new GenericKeyedObjectPool<>(pooledObjectFactory, config);
		genericKeyedObjectPool.setTestOnBorrow(true);
		genericKeyedObjectPool.setTestOnCreate(true);
		this.objectPool = genericKeyedObjectPool;
	}
	
	@Override
	public V borrowObject(K key) throws BorrowObjectException{
		
		try {
			V value = this.objectPool.borrowObject(key);
			logger.info("[borrowObject]{}, {}", key, value);
			return value;
		} catch (Exception e) {
			logger.error("[borrowObject]" + key,  e);
			throw new BorrowObjectException("borrow " + key, e);
		}
	}
	
	@Override
	public void returnObject(K key, V value) throws ReturnObjectException{
		
		try {
			logger.info("[returnObject]{}, {}", key, value);
			this.objectPool.returnObject(key, value);
		} catch (Exception e) {
			logger.error("[returnObject]", e);
			throw new ReturnObjectException("return " + key + " " + value , e);
		}
	}
	

	@Override
	protected void doDispose() throws Exception {
		this.objectPool.clear();
	}


	@Override
	public void clear() throws Exception {
		this.objectPool.clear();
	}

	@Override
	public void clear(K key) throws Exception {
		this.objectPool.clear(key);
	}

	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE;
	}

}
