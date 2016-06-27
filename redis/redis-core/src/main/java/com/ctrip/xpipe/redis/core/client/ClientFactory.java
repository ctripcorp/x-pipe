package com.ctrip.xpipe.redis.core.client;

import java.net.InetSocketAddress;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Jun 27, 2016
 */
public class ClientFactory implements KeyedPooledObjectFactory<InetSocketAddress, Client>{
	
	private Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public PooledObject<Client> makeObject(InetSocketAddress key) throws Exception {
		logger.info("[makeObject]{}", key);
	
		Client client = new Client(key);
		client.initialize();
		client.start();
		return new DefaultPooledObject<Client>(client);
	}

	@Override
	public void destroyObject(InetSocketAddress key, PooledObject<Client> p) throws Exception {
		
		logger.info("[destroyObject]{}, {}", key, p.getObject());
		p.getObject().stop();
		p.getObject().dispose();
	}

	@Override
	public boolean validateObject(InetSocketAddress key, PooledObject<Client> p) {
		
		Client client = p.getObject();
		return client.isAlive();
	}

	@Override
	public void activateObject(InetSocketAddress key, PooledObject<Client> p) throws Exception {
		
	}

	@Override
	public void passivateObject(InetSocketAddress key, PooledObject<Client> p) throws Exception {
		
	}


}
