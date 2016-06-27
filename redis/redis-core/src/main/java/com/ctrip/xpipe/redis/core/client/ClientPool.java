package com.ctrip.xpipe.redis.core.client;

import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;


/**
 * @author wenchao.meng
 *
 * Jun 27, 2016
 */
public class ClientPool {
	
	private static  ClientPool clientPool;
	
	private KeyedObjectPool<InetSocketAddress, Client> clients;
	
	private ClientPool(){
		GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
		config.setBlockWhenExhausted(false);
		clients = new GenericKeyedObjectPool<InetSocketAddress, Client>(new ClientFactory(), config);
	}
	
	public Client getClient(InetSocketAddress address) throws NoSuchElementException, IllegalStateException, Exception{
		return clients.borrowObject(address);
	}
	
	public static ClientPool getInstance(){
		
		if(clientPool != null){
			return clientPool;
		}
		
		synchronized (ClientPool.class) {
			if(clientPool != null){
				return clientPool;
			}
			clientPool = new ClientPool();
		}
		return clientPool;
	}

}
