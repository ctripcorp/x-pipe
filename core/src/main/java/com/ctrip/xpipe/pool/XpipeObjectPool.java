package com.ctrip.xpipe.pool;



import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class XpipeObjectPool<T> extends AbstractLifecycle implements SimpleObjectPool<T>{
	
	private ObjectPool<T> objectPool;
	private PooledObjectFactory<T> factory; 
	private GenericObjectPoolConfig  config;

	public XpipeObjectPool(PooledObjectFactory<T> factory) {
		this(factory, new GenericObjectPoolConfig());
	}

	public XpipeObjectPool(PooledObjectFactory<T> factory, GenericObjectPoolConfig  config) {
		
		this.factory = factory;
		this.config = config;
	}
	
	@Override
	protected void doInitialize() throws Exception {
		objectPool = new GenericObjectPool<>(factory, config);
	}
	
	public T borrowObject() throws BorrowObjectException {
		
		try {
			return objectPool.borrowObject();
		} catch (Exception e) {
			logger.error("[borrowObject]" + factory,  e);
			throw new BorrowObjectException("borrow " + factory, e);
		}
	}
	
	public void returnObject(T obj) throws ReturnObjectException{
		
		try{
			objectPool.returnObject(obj);
		} catch (Exception e) {
			logger.error("[returnObject]", e);
			throw new ReturnObjectException("return " + obj , e);
		}
	}
	
	@Override
	protected void doDispose() throws Exception {
		objectPool.clear();
	}
}
