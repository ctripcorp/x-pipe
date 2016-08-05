package com.ctrip.xpipe.redis.meta.server.impl.listener;




import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.impl.AbstractMetaChangeListener;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
@Component
public class ConsoleListener extends AbstractMetaChangeListener{
	
	@Autowired
	private MetaServerConfig metaServerConfig;
	
	private String dc = FoundationService.DEFAULT.getDataCenter();
	
	private RestTemplate restTemplate = new RestTemplate();
	
	

	@Override
	protected void redisMasterChanged(String clusterId, String shardId, RedisMeta oldRedisMaster, RedisMeta newRedisMaster) {

		logger.info("[redisMasterChanged]{},{},{}->{}", clusterId, shardId, oldRedisMaster, newRedisMaster);

		String target = String.format("%s/api/v1/%s/%s/%s/redis/master", metaServerConfig.getConsoleAddress(), dc, clusterId, shardId);
		request(target, newRedisMaster);
		
	}

	private void request(String target, Object param) {
		
		try{
			restTemplate.postForObject(target, param, String.class);
		}catch(Exception e){
			if(e instanceof ResourceAccessException){
				logger.error("[request]" + e.getMessage());
			}else{
				logger.error("[request]", e);
			}
		}
	}

	@Override
	protected void activeKeeperChanged(String clusterId, String shardId, KeeperMeta oldKeeperMeta, KeeperMeta newKeeperMeta) {
		
		if(newKeeperMeta == null){
			newKeeperMeta = new KeeperMeta();
		}
		logger.info("[activeKeeperChanged]{},{},{}->{}", clusterId, shardId, oldKeeperMeta, newKeeperMeta);

		String target = String.format("%s/api/v1/%s/%s/%s/keeper/active", metaServerConfig.getConsoleAddress(), dc, clusterId, shardId);
		request(target, newKeeperMeta);
	}

}
