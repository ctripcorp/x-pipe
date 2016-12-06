package com.ctrip.xpipe.pool;

import java.net.InetSocketAddress;

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
		implements TopElement, SimpleKeyedObjectPool<InetSocketAddress, NettyClient> {

	private KeyedObjectPool<InetSocketAddress, NettyClient> objectPool;
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

	@Override
	protected void doInitialize() throws Exception {

		this.pooledObjectFactory = new NettyKeyedPoolClientFactory();
		pooledObjectFactory.start();

		GenericKeyedObjectPool<InetSocketAddress, NettyClient> genericKeyedObjectPool = new GenericKeyedObjectPool<>(
				pooledObjectFactory, config);
		genericKeyedObjectPool.setTestOnBorrow(true);
		genericKeyedObjectPool.setTestOnCreate(true);
		this.objectPool = genericKeyedObjectPool;
	}
	
	@Override
	public SimpleObjectPool<NettyClient> getKeyPool(InetSocketAddress key){
		return new XpipeObjectPoolFromKeyed<InetSocketAddress, NettyClient>(this, key);
	}

	@Override
	public NettyClient borrowObject(InetSocketAddress key) throws BorrowObjectException {

		try {
			NettyClient value = this.objectPool.borrowObject(key);
			logger.info("[borrowObject]{}, {}", key, value);
			return value;
		} catch (Exception e) {
			logger.error("[borrowObject]" + key, e);
			throw new BorrowObjectException("borrow " + key, e);
		}
	}

	@Override
	public void returnObject(InetSocketAddress key, NettyClient value) throws ReturnObjectException {

		try {
			logger.info("[returnObject]{}, {}", key, value);
			this.objectPool.returnObject(key, value);
		} catch (Exception e) {
			logger.error("[returnObject]", e);
			throw new ReturnObjectException("return " + key + " " + value, e);
		}
	}

	@Override
	protected void doDispose() throws Exception {
		logger.info("[XpipeNettyClientKeyedObjectPool][doDispose]");
		this.objectPool.close();
		this.pooledObjectFactory.stop();

	}

	@Override
	public void clear() throws Exception {
		this.objectPool.clear();
	}

	@Override
	public void clear(InetSocketAddress key) throws Exception {
		this.objectPool.clear(key);
	}

	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE;
	}

}
