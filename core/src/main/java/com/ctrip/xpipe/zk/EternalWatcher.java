package com.ctrip.xpipe.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.WatchedEvent;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;

/**
 * @author wenchao.meng
 *
 * Sep 21, 2016
 */
public class EternalWatcher extends AbstractStartStoppable implements Releasable{
	
	public static enum WATCHER_TYPE {
		EXISTS,
		GET_CHILDREN
	}
	
	private CuratorFramework client;
	private CuratorWatcher realWatcher;
	private String path;
	private WATCHER_TYPE watcherType = WATCHER_TYPE.GET_CHILDREN;
	
	private CuratorWatcher parentWatcher;
	private ConnectionStateListener connectionStateListener;
	
	public EternalWatcher(CuratorFramework client, CuratorWatcher curatorWatcher, String path) {
		
		this.client = client;
		this.realWatcher = curatorWatcher;
		this.path = path;
		
		parentWatcher = new CuratorWatcher() {
			
			@Override
			public void process(WatchedEvent event) throws Exception {
				
				addWatcher();
				if(isStarted()){
					realWatcher.process(event);
				}
			}

			@Override
			public String toString() {
				return String.format("parentWatcher, real:%s", realWatcher.toString());
			}
		};
		
		
		connectionStateListener = new ConnectionStateListener() {
			
			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				
				if(newState == ConnectionState.RECONNECTED){
					try {
						addWatcher();
					} catch (Exception e) {
						logger.error("[stateChanged][add watcher]" + parentWatcher, e);
					}
				}
			}
		};
	}

	protected void addWatcher() throws Exception {
		
		if(isStarted()){
			logger.info("[addWatcher]path:{}, {}", path, parentWatcher);
			switch (watcherType) {
			case GET_CHILDREN:
				client.getChildren().usingWatcher(parentWatcher).forPath(path);
				break;
			case EXISTS:
				client.checkExists().usingWatcher(parentWatcher).forPath(path);
				break;
			default:
				throw new IllegalStateException("unknown type:" + watcherType);
			}
		}else{
			logger.info("[addWatcher][stopped, do not watch]{}", parentWatcher);
		}
	}

	@Override
	protected void doStart() throws Exception {
		
		client.getConnectionStateListenable().addListener(connectionStateListener);
		addWatcher();
	}

	@Override
	protected void doStop() {
		client.getConnectionStateListenable().removeListener(connectionStateListener);
	}

	@Override
	public void release() throws Exception {
		stop();
	}
	
	@Override
	public String toString() {
		return String.format("%s(%s)", getClass().getSimpleName(), realWatcher);
	}
}
