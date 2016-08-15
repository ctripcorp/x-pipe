package com.ctrip.xpipe.redis.keeper.meta;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.google.common.base.Function;

/**
 * @author marsqing
 *
 *         May 30, 2016 2:19:44 PM
 */
@Component
public class DefaultMetaService implements MetaServerKeeperService {

	private static Logger logger = LoggerFactory.getLogger(DefaultMetaService.class);

	@SuppressWarnings("unused")
	@Autowired
	private KeeperConfig config;

	@Autowired
	private MetaServerLocator metaServerLocator;
	
	private RestTemplate restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate();
	
	@Override
	public ShardStatus getShardStatus(String clusterId, String shardId) {
		
		return getRequestToMetaServer(String.format("%s/%s", PATH_PREFIX, PATH_SHARD_STATUS), clusterId, shardId);

	}

	public ShardStatus getRequestToMetaServer(final String path, final Object ...urlVariables) {
		
		return pollMetaServer(new Function<String, ShardStatus>() {

			@Override
			public ShardStatus apply(String baseUrl) {
				
				String url = String.format("%s%s", baseUrl, path);
				try{
					return restTemplate.getForObject(url, ShardStatus.class, urlVariables);
				}catch(Exception e){
					logger.error("[apply]"+ url, e);
					return null;
				}
			}
		});
	}

	private <T> T pollMetaServer(Function<String, T> fun) {
		List<String> metaServerList = metaServerLocator.getMetaServerList();

		for (String url : metaServerList) {
			T result = fun.apply(url);
			if (result != null) {
				return result;
			} else {
				continue;
			}
		}
		return null;
	}

	public void setConfig(KeeperConfig config) {
		this.config = config;
	}
	
	public void setMetaServerLocator(MetaServerLocator metaServerLocator) {
		this.metaServerLocator = metaServerLocator;
	}

	@Override
	public List<MetaServerMeta> getAllMetaServers() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public void ping(String clusterId, String shardId, KeeperInstanceMeta keeperInstanceMeta) {
		
	}

	@Override
	public List<KeeperTransMeta> getAllKeepersByKeeperContainer(KeeperContainerMeta keeperContainerMeta) {
		// TODO Auto-generated method stub
		return null;
	}

}