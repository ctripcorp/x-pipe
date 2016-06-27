package com.ctrip.xpipe.redis.core.client;

import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author wenchao.meng
 *
 * Jun 27, 2016
 */
public class ClientPool {
	
	private static final Logger logger = LoggerFactory.getLogger(ClientPool.class);
	
	private static  ClientPool clientPool;
	
	private KeyedObjectPool<InetSocketAddress, Client> clients;
	
	private ClientPool(){
		GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
		config.setBlockWhenExhausted(false);
		clients = new GenericKeyedObjectPool<InetSocketAddress, Client>(new ClientFactory(), config);
	}
	
	public Client borrowClient(InetSocketAddress address) throws NoSuchElementException, IllegalStateException, Exception{
		
		Client client = clients.borrowObject(address);
		logger.info("[borrowClient]{}", client);
		return client;
	}
	
	public void returnClient(Client client){
		try {
			logger.info("[returnClient]{}", client);
			clients.returnObject(client.getAddress(), client);
		} catch (Exception e) {
			logger.error("[returnClient]" + client, e);
			throw new IllegalStateException("[returnClient]" + client, e);
		}
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
