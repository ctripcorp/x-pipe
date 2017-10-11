package com.ctrip.xpipe.redis.keeper.meta;

import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerLocator;
import com.ctrip.xpipe.redis.core.metaserver.impl.AbstractMetaService;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.google.common.base.Function;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author marsqing
 *
 *         May 30, 2016 2:19:44 PM
 */
@Component
public class DefaultMetaService extends AbstractMetaService implements MetaServerKeeperService {


	@SuppressWarnings("unused")
	@Autowired
	private KeeperConfig config;

	@Autowired
	private MetaServerLocator metaServerLocator;
	
	
	public ShardStatus getRequestToMetaServer(final String path, final Object ...urlVariables) {
		
		return pollMetaServer(new Function<String, ShardStatus>() {

			@Override
			public ShardStatus apply(String baseUrl) {
				
				String url = String.format("%s%s", baseUrl, path);
				try{
					return restTemplate.getForObject(url, ShardStatus.class, urlVariables);
				}catch(Exception e){
					
					if(ExceptionUtils.isSocketIoException(e)){
						logger.error("[apply]{},{}", url, e);
					}else{
						logger.error("[apply]"+ url, e);
					}
					return null;
				}
			}
		});
	}

	public void setConfig(KeeperConfig config) {
		this.config = config;
	}
	
	public void setMetaServerLocator(MetaServerLocator metaServerLocator) {
		this.metaServerLocator = metaServerLocator;
	}

	@Override
	public void ping(String clusterId, String shardId, KeeperInstanceMeta keeperInstanceMeta) {
		
	}

	@Override
	public List<KeeperTransMeta> getAllKeepersByKeeperContainer(KeeperContainerMeta keeperContainerMeta) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<String> getMetaServerList() {
		return metaServerLocator.getMetaServerList();
	}
}