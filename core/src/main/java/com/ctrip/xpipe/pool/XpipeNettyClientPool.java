package com.ctrip.xpipe.pool;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.ObjectPoolException;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientFactory;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class XpipeNettyClientPool extends AbstractLifecycle implements SimpleObjectPool<NettyClient>{
	
	private ObjectPool<NettyClient> objectPool;
	private PooledObjectFactory<NettyClient> factory;
	private GenericObjectPoolConfig  config;
	private Endpoint target;

	public XpipeNettyClientPool(Endpoint target) {
		this(target, createDefaultPoolConfig());
	}

	public XpipeNettyClientPool(Endpoint target, PooledObjectFactory<NettyClient> factory) {
		this(target, createDefaultPoolConfig(), factory);
	}

	public XpipeNettyClientPool(Endpoint target, GenericObjectPoolConfig  config) {
		this.target = target;
		this.config = config;
	}

	public XpipeNettyClientPool(Endpoint target, GenericObjectPoolConfig config, PooledObjectFactory<NettyClient> factory) {
		this.factory = factory;
		this.config = config;
		this.target = target;
	}

	@Override
	protected void doInitialize() throws Exception {
		if(factory == null) {
			NettyClientFactory factory = new NettyClientFactory(target, false);
			factory.start();
			this.factory = factory;
		}
		
		GenericObjectPool<NettyClient> genericObjectPool = new GenericObjectPool<>(factory, config);
		genericObjectPool.setTestOnBorrow(true);
		genericObjectPool.setTestOnCreate(true);
		this.objectPool = genericObjectPool;
	}
	
	public NettyClient borrowObject() throws BorrowObjectException {
		
		try {
			return objectPool.borrowObject();
		} catch (Exception e) {
			logger.error("[borrowObject] NumIdle:{}, NumActive:{}" + factory, objectPool.getNumIdle(), objectPool.getNumActive(), e);
			throw new BorrowObjectException("borrow " + factory, e);
		}
	}
	
	public void returnObject(NettyClient obj) throws ReturnObjectException{
		
		try{
			objectPool.returnObject(obj);
		} catch (Exception e) {
			logger.error("[returnObject]", e);
			throw new ReturnObjectException("return " + obj , e);
		}
	}
	
	@Override
	protected void doDispose() throws Exception {
		if(this.factory instanceof NettyClientFactory) {
			((NettyClientFactory)this.factory).stop();
		}
		objectPool.close();
	}

	@Override
	public void clear() throws ObjectPoolException {
		try {
			this.objectPool.clear();
		} catch (Exception e) {
			throw new ObjectPoolException("clear:" + objectPool, e);
		}
	}

	@Override
	public String desc() {
		return factory.toString();
	}

	@VisibleForTesting
	public ObjectPool getObjectPool() {
		return objectPool;
	}

	private static GenericObjectPoolConfig createDefaultPoolConfig() {
		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		config.setJmxEnabled(false);
		return config;
	}
}
