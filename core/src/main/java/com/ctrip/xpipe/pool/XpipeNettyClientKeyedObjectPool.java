package com.ctrip.xpipe.pool;


import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.ObjectPoolException;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.springframework.core.Ordered;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;

/**
 * let pool implements lifecycle
 * 
 * @author wenchao.meng
 *
 *         Jun 28, 2016
 */
public class XpipeNettyClientKeyedObjectPool extends AbstractLifecycle
		implements TopElement, SimpleKeyedObjectPool<Endpoint, NettyClient> {

	public static final long DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS = 30000;
	public static final long DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS = 1000L * 60L * 30L;
	public static final long DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS = 5000L;


	private KeyedObjectPool<Endpoint, NettyClient> objectPool;
	private NettyKeyedPoolClientFactory pooledObjectFactory;
	private GenericKeyedObjectPoolConfig config;

	public XpipeNettyClientKeyedObjectPool() {
		this(createDefaultConfig(GenericKeyedObjectPoolConfig.DEFAULT_MAX_TOTAL_PER_KEY));
	}

	public XpipeNettyClientKeyedObjectPool(int maxPerKey) {
		this(createDefaultConfig(maxPerKey));
	}

	private static GenericKeyedObjectPoolConfig createDefaultConfig(int maxPerKey) {
		GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
		config.setMaxTotalPerKey(maxPerKey);
		config.setBlockWhenExhausted(false);
		return config;
	}

	public XpipeNettyClientKeyedObjectPool(GenericKeyedObjectPoolConfig config) {
		this.config = config;

	}

	public XpipeNettyClientKeyedObjectPool(NettyKeyedPoolClientFactory pooledObjectFactory) {
		this(createDefaultConfig(GenericKeyedObjectPoolConfig.DEFAULT_MAX_TOTAL_PER_KEY));
		this.pooledObjectFactory = pooledObjectFactory;
	}

	@Override
	protected void doInitialize() throws Exception {

		if(this.pooledObjectFactory == null) {
			this.pooledObjectFactory = new NettyKeyedPoolClientFactory();
		}
		pooledObjectFactory.start();

		GenericKeyedObjectPool<Endpoint, NettyClient> genericKeyedObjectPool = new GenericKeyedObjectPool<Endpoint, NettyClient>(
				pooledObjectFactory, config);
		genericKeyedObjectPool.setTestOnBorrow(true);
		genericKeyedObjectPool.setTestOnCreate(true);
		genericKeyedObjectPool.setSoftMinEvictableIdleTimeMillis(DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
		genericKeyedObjectPool.setMinEvictableIdleTimeMillis(DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
		genericKeyedObjectPool.setTimeBetweenEvictionRunsMillis(DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
		this.objectPool = genericKeyedObjectPool;
	}

	public final void setKeyPooConfig(int minIdlePerKey, long softMinEvictableIdleTimeMillis, long minEvictableIdleTimeMillis, long timeBetweenEvictionRunsMillis) {

		if(objectPool instanceof GenericKeyedObjectPool){
			logger.info("[setKeyPooConfig]{}, {}, {}, {}", minIdlePerKey, softMinEvictableIdleTimeMillis, minEvictableIdleTimeMillis, timeBetweenEvictionRunsMillis);
			GenericKeyedObjectPool genericKeyedObjectPool = (GenericKeyedObjectPool) objectPool;
			genericKeyedObjectPool.setMinIdlePerKey(minIdlePerKey);
			genericKeyedObjectPool.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
			genericKeyedObjectPool.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
			genericKeyedObjectPool.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
		}else {
			logger.warn("[setKeyPooConfig][not generickeyedobjectpool]");
		}
	}



	@Override
	public SimpleObjectPool<NettyClient> getKeyPool(Endpoint key){
		return new XpipeObjectPoolFromKeyed<Endpoint, NettyClient>(this, key);
	}

	@Override
	public NettyClient borrowObject(Endpoint key) throws BorrowObjectException {

		try {
			logger.debug("[borrowObject][begin]{}", key);
			NettyClient value = this.objectPool.borrowObject(key);
			logger.debug("[borrowObject][end]{}, {}", key, value);
			return value;
		} catch (Exception e) {
			logger.error("[borrowObject]" + key, e);
			throw new BorrowObjectException("borrow " + key, e);
		}
	}

	@Override
	public void returnObject(Endpoint key, NettyClient value) throws ReturnObjectException {

		try {
			logger.debug("[returnObject]{}, {}", key, value);
			this.objectPool.returnObject(key, value);
		} catch (Exception e) {
			logger.error("[returnObject]", e);
			throw new ReturnObjectException("return " + key + " " + value, e);
		}
	}

	@Override
	protected void doDispose() throws Exception {

		this.objectPool.close();
		this.pooledObjectFactory.stop();

	}

	@Override
	public void clear() throws ObjectPoolException {
		try {
			this.objectPool.clear();
		} catch (Exception e) {
			throw new ObjectPoolException("clear " + objectPool, e);
		}
	}

	@Override
	public void clear(Endpoint key) throws ObjectPoolException {
		try {
			this.objectPool.clear(key);
		} catch (Exception e) {
			throw new ObjectPoolException("object pool:" + objectPool + ",key:" + key, e);
		}
	}

	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE;
	}
	
	//for test
	public KeyedObjectPool<Endpoint, NettyClient> getObjectPool() {
		return objectPool;
	}

}
