package com.ctrip.xpipe.pool;


import java.util.NoSuchElementException;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

/**
 * let pool implements lifecycle
 * @author wenchao.meng
 *
 * Jun 28, 2016
 */
public class XpipeKeyedObjectPool<K, V> extends AbstractLifecycle implements TopElement{
	
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
		this.objectPool = new GenericKeyedObjectPool<>(pooledObjectFactory, config);
	}
	
	public V borrowObject(K key) throws NoSuchElementException, IllegalStateException, Exception{
		
		V value = this.objectPool.borrowObject(key);
		logger.info("[borrowObject]{}, {}", key, value);
		return value;
	}
	
	public void returnObject(K key, V value) throws Exception{
		
		logger.info("[returnObject]{}, {}", key, value);
		this.objectPool.returnObject(key, value);
	}
	

	@Override
	protected void doDispose() throws Exception {
		this.objectPool.clear();
	}

}
