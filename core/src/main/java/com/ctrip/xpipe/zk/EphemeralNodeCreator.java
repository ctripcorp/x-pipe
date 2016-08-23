package com.ctrip.xpipe.zk;



import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

/**
 * @author wenchao.meng
 *
 * Aug 23, 2016
 */
public class EphemeralNodeCreator implements Startable, Stoppable{
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private CuratorFramework client;
	private String path;
	private byte []data;
	private NodeTheSame nodeTheSame;
	private ConnectionStateListener connectionStateListener = new ConnectionStateListener() {
		
		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			if(newState == ConnectionState.RECONNECTED){
				try {
					logger.info("[stateChanged]{}", newState);
					doCreate();
				} catch (Exception e) {
					logger.error("[doCreate]" + path, e);
				}
			}
		}
	};
	
	private AtomicReference<Boolean> started = new AtomicReference<Boolean>(false); 
	
	public EphemeralNodeCreator(CuratorFramework client, String path, byte []data, NodeTheSame nodeTheSame){
		
		this.client = client;
		this.path = path;
		this.data = data;
		this.nodeTheSame = nodeTheSame;
	}
	
	
	@Override
	public void stop() throws Exception {
		
		if(!started.compareAndSet(true, false)){
			throw new Exception("not started yet!");
		}

		
		byte []zkData = client.getData().forPath(path);
		if(nodeTheSame.same(zkData)){
			
			logger.info("[stop][delete path]{}", path);
			client.delete().forPath(path);
			client.getConnectionStateListenable().removeListener(connectionStateListener);
		}
	}

	@Override
	public void start() throws Exception {
		
		if(!started.compareAndSet(false, true)){
			throw new Exception("already started!");
		}
		
		doCreate();
		client.getConnectionStateListenable().addListener(connectionStateListener);
	}

	private void doCreate() throws Exception {
		
		try{
			if(!started.get()){
				logger.info("[doCreate][stopped]{}", path);
				return;
			}
			logger.info("[doCreate]{}, {}", path, new String(data));
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, data);
			client.checkExists().usingWatcher(new CuratorWatcher() {
				
				@Override
				public void process(WatchedEvent event) throws Exception {
					
					if(event.getType() == EventType.NodeDeleted){
						
						try{
							logger.info("[process][node deleted]{}", path);
							doCreate();
						}catch(Exception e){
							logger.info("[process]" + event, e);
							client.checkExists().usingWatcher(this).forPath(path);
						}
					}else{
						client.checkExists().usingWatcher(this).forPath(path);
					}
				}
			}).forPath(path);
		}catch(NodeExistsException e){
			logger.info("[doCreate][already exists]{}, {}", path, new String(data));
			checkReplace();
		} 
	}

	private void checkReplace() throws Exception {

		logger.info("[doCreate][already exists]{}, {}", path, new String(data));
		byte []zkData = client.getData().forPath(path);
		
		if(nodeTheSame.same(zkData)){
			logger.info("[doCreate][replace, delete]{}, {}", path, new String(data));
			client.delete().forPath(path);
			doCreate();
		}else{
			throw new EphemeralNodeCanNotReplaceException(path, zkData, data);
		}
	}

}
