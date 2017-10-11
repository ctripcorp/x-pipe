package com.ctrip.xpipe.redis.meta.server.impl;


import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.MetaHolder;
import org.junit.Before;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
@SuppressWarnings("unused")
public class DefaultMetaServerTest extends AbstractMetaServerTest{
	
	private DefaultMetaServer defaultMetaServer = new DefaultMetaServer();
	private FakeMetaHolder fakeMetaHolder = new FakeMetaHolder();
	private int zkPort = randomPort(2181, 2281);
	
	@Before
	public void beforeDefaultMetaServerTest() throws Exception{
		
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
