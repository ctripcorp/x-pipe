package com.ctrip.xpipe.pool;

import java.net.InetSocketAddress;

import com.ctrip.xpipe.api.pool.ObjectPoolException;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientFactory;
/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class XpipeNettyClientPool extends AbstractLifecycle implements SimpleObjectPool<NettyClient>{
	
	private ObjectPool<NettyClient> objectPool;
	private NettyClientFactory factory; 
	private GenericObjectPoolConfig  config;
	private InetSocketAddress target;

	public XpipeNettyClientPool(InetSocketAddress target) {
		this(target, new GenericObjectPoolConfig());
	}

	public XpipeNettyClientPool(InetSocketAddress target, GenericObjectPoolConfig  config) {
		this.target = target;
		this.config = config;
	}
	
	@Override
	protected void doInitialize() throws Exception {
		
		this.factory = new NettyClientFactory(target);
		this.factory.start();
		
		GenericObjectPool<NettyClient> genericObjectPool = new GenericObjectPool<>(factory, config);
		genericObjectPool.setTestOnBorrow(true);
		genericObjectPool.setTestOnCreate(true);
		this.objectPool = genericObjectPool;
	}
	
	public NettyClient borrowObject() throws BorrowObjectException {
		
		try {
			return objectPool.borrowObject();
		} catch (Exception e) {
			logger.error("[borrowObject]" + factory,  e);
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
		
		this.factory.stop();
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
}
