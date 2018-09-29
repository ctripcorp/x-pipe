package com.ctrip.xpipe.redis.core.protocal.cmd.manual;

import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientFactory;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.After;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *
 * Oct 28, 2016
 */
public class AbstractCommandTest extends AbstractRedisTest{
	
	private List<Stoppable> stoppables = new LinkedList<>();

	protected FixedObjectPool<NettyClient> createClientPool(String host, int port) throws Exception {

		NettyClientFactory nettyClientFactory = new NettyClientFactory(new DefaultEndPoint(host, port));
		nettyClientFactory.start();
		stoppables.add(nettyClientFactory);
		
		NettyClient nettyClient = nettyClientFactory.makeObject().getObject();
		FixedObjectPool<NettyClient> clientPool = new FixedObjectPool<NettyClient>(nettyClient);
		return clientPool;
	}
	
	
	@After
	public void afterAbstractCommandTest(){
		
		for(Stoppable stoppable : stoppables){
			try {
				stoppable.stop();
			} catch (Exception e) {
				logger.error("[afterAbstractCommandTest]" + stoppable, e);
			}
		}
	}

}
