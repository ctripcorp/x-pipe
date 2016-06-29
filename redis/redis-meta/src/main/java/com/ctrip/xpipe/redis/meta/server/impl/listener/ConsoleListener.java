package com.ctrip.xpipe.redis.meta.server.impl.listener;


import java.io.IOException;

import javax.ws.rs.ProcessingException;
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

		logger.info("[redisMasterChanged]{},{},{}->{}", clusterId, shardId, oldRedisMaster, newRedisMaster);

		String target = String.format("%s/api/v1/%s/%s/%s/redis/master", metaServerConfig.getConsoleAddress(), dc, clusterId, shardId);
		request(target, newRedisMaster);
		
	}

	private void request(String target, Object param) {
		
		try{
			Response response =  RestRequestClient.post(target, param);
			if(response.getStatus() != HttpConstants.HTTP_STATUS_200){
				logger.error("[request][call console failed!]" + response);
			}
		}catch(ProcessingException e){
			if(e.getCause() instanceof IOException){
				logger.error("[request]" + e.getMessage());
			}else{
				logger.error("[request]", e);
			}
		} catch (Exception e) {
			logger.error("[request]", e);
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
