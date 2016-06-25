/**
 * 
 */
package com.ctrip.xpipe.redis.meta.server.impl;

import java.io.ByteArrayInputStream;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.redis.meta.server.MetaHolder;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author marsqing
 *
 *         Jun 12, 2016 3:13:52 PM
 */
@Component
public class DefaultMetaHolder extends AbstractLifecycleObservable implements MetaHolder, CuratorWatcher {

	private static Logger log = LoggerFactory.getLogger(DefaultMetaHolder.class);

	@Autowired
	private ZkClient zkClient;

	@SuppressWarnings("unused")
	@Autowired
	private MetaServerConfig config;
	
	private String metaPath = MetaZkConfig.getMetaRootPath();


	private XpipeMeta meta;

	@Override
	protected void doStart() throws Exception {

		zkClient.get().checkExists().usingWatcher(this).forPath(metaPath);
		
		getMetaFromZk();
	}
	

	@Override
	protected void doStop() throws Exception {
		
	}

	private void getMetaFromZk() throws Exception {
		
		byte[] metaBytes = zkClient.get().getData().forPath(metaPath);
		
		if (metaBytes == null || metaBytes.length == 0) {
			log.error("Meta not found in zk, path {}", metaPath);
			throw new RuntimeException("meta not found in zk, path " + metaPath);
		} else {
			try {
				meta = DefaultSaxParser.parse(new ByteArrayInputStream(metaBytes));
			} catch (Exception e) {
				log.error("Can not read meta from zk, path {}", metaPath);
				throw new RuntimeException("Can not read meta from zk, path " + metaPath);
			}
		}
	}

	@Override
	public XpipeMeta getMeta() {
		return meta;
	}


	@Override
	public void process(WatchedEvent event) throws Exception {
		
		if(event.getType() == EventType.NodeDataChanged){
			getMetaFromZk();
			notifyObservers(meta);
		}
		zkClient.get().checkExists().usingWatcher(this).forPath(metaPath);
	}

}
