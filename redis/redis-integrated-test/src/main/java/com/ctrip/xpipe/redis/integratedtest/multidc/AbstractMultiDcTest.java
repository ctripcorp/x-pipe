package com.ctrip.xpipe.redis.integratedtest.multidc;




import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.springframework.context.ApplicationContext;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.integratedtest.AbstractIntegratedTestTemplate;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.meta.server.DefaultMetaServer;

/**
 * @author wenchao.meng
 *
 * Jun 15, 2016
 */
public abstract class AbstractMultiDcTest extends AbstractIntegratedTestTemplate{
	

	@Before
	public void beforeAbstractMultiDcTest() throws Exception{

		for(DcMeta dcMeta : getDcMetas()){
			
			startDc(dcMeta.getId());
		}
		updateUpstreamKeeper();
		sleep(6000);
	}
	
	@Override
	protected List<RedisMeta> getRedisSlaves() {
		
		List<RedisMeta> result = new LinkedList<>();
		for(DcMeta dcMeta : getDcMetas()){
			List<RedisMeta> slaves = getRedisSlaves(dcMeta.getId());
			Assert.assertTrue(slaves.size() >= 1);
			result.addAll(slaves);
		}
		Assert.assertTrue(result.size() >= 1);
		return result;
	}

	/**
	 * wait for console ready, to be deleted!!
	 */
	private void updateUpstreamKeeper() {
		
		sleep(5000);
		
		DcMeta activeDc = activeDc();
		
		RedisKeeperServer activeRedisKeeperServer = null;
		for(RedisKeeperServer redisKeeperServer : getRedisKeeperServers(activeDc.getId())){
			if(redisKeeperServer.getRedisKeeperServerState().isActive()){
				activeRedisKeeperServer = redisKeeperServer;
			}
		}

		for(DcMeta dcMeta : backupDcs()){
			
			DcInfo dcInfo = getDcInfos().get(dcMeta.getId());
			ApplicationContext applicationContext = dcInfo.getApplicationContext();
			DefaultMetaServer metaServer = applicationContext.getBean(DefaultMetaServer.class);
			
			metaServer.updateUpstreamKeeper(activeRedisKeeperServer.getClusterId(), activeRedisKeeperServer.getShardId(), activeRedisKeeperServer.getCurrentKeeperMeta());
		}
		
	}

}
