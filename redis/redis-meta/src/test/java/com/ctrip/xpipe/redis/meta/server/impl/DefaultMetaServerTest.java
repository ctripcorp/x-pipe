package com.ctrip.xpipe.redis.meta.server.impl;

import java.util.LinkedList;

import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.MetaHolder;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public class DefaultMetaServerTest extends AbstractMetaServerTest{
	
	private DefaultMetaServer defaultMetaServer = new DefaultMetaServer();
	private FakeMetaHolder fakeMetaHolder = new FakeMetaHolder();
	private int zkPort = randomPort(2181, 2281);
	
	@Before
	public void beforeDefaultMetaServerTest() throws Exception{
		
		defaultMetaServer.setConfig(new DefaultMetaServerConfig());
		defaultMetaServer.setMetaChangeListeners(new LinkedList<MetaChangeListener>());
		defaultMetaServer.setMetaHolder(fakeMetaHolder);
		
		DefaultZkClient zkClient = new DefaultZkClient();		
		zkClient.setZkAddress("127.0.0.1:" + zkPort);
		defaultMetaServer.setZkClient(zkClient);
		
		startZk(zkPort);
		
		defaultMetaServer.initialize();
		defaultMetaServer.start();
	}
	
	@Test
	public void testElect(){
		
		
	}
	
	
	
	public class FakeMetaHolder extends AbstractLifecycleObservable implements MetaHolder{

		@Override
		public XpipeMeta getMeta() {
			return getXpipeMeta();
		}
		
	}

}
