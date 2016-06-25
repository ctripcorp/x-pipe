package com.ctrip.xpipe.redis.meta.server.impl.listener;


import javax.ws.rs.core.Response;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.http.HttpConstants;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.impl.AbstractMetaChangeListener;
import com.ctrip.xpipe.rest.RestRequestClient;
import com.ctrip.xpipe.utils.ServicesUtil;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
@Component
public class ConsoleListener extends AbstractMetaChangeListener{
	
	@Autowired
	private MetaServerConfig metaServerConfig;
	
	private String dc = ServicesUtil.getFoundationService().getDataCenter();
	
	

	@Override
	protected void redisMasterChanged(String clusterId, String shardId, RedisMeta oldRedisMaster, RedisMeta newRedisMaster) {
		
	}

	@Override
	protected void activeKeeperChanged(String clusterId, String shardId, KeeperMeta oldKeeperMeta, KeeperMeta newKeeperMeta) {
		
		if(newKeeperMeta == null){
			newKeeperMeta = new KeeperMeta();
		}

		String target = String.format("%s/api/v1/%s/%s/%s/keeper/active", metaServerConfig.getConsoleAddress(), dc, clusterId, shardId);
		Response response =  RestRequestClient.request(target, newKeeperMeta);
		if(response.getStatus() != HttpConstants.HTTP_STATUS_200){
			logger.error("[activeKeeperChanged][call console]" + response);
		}
	}

}
