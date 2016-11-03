package com.ctrip.xpipe.redis.meta.server.multidc;

import java.net.InetSocketAddress;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

/**
 * @author wenchao.meng
 *
 * Nov 3, 2016
 */
public class MultiDcNotifier implements MetaServerStateChangeHandler{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	private MetaServerConfig metaServerConfig;

	@Autowired
	private MetaServerMultiDcServiceManager MetaServerMultiDcServiceManager;
	
	@Autowired
	public DcMetaCache dcMetaCache;
	
	@Override
	public void keeperActiveElected(String clusterId, String shardId, KeeperMeta activeKeeper) throws Exception {
		
		if(!dcMetaCache.isCurrentDcPrimary(clusterId, shardId)){
			logger.info("[keeperActiveElected][current dc backup, do nothing]{}, {}", clusterId, shardId, activeKeeper);
			return;
		}
		
		logger.info("[keeperActiveElected][current dc primary, notify backup dc]{}, {}", clusterId, shardId, activeKeeper);
		Map<String, DcInfo> dcInfos = metaServerConfig.getDcInofs();
		
	}

	@Override
	public void keeperMasterChanged(String clusterId, String shardId, InetSocketAddress newMaster) {
		
	}

}
