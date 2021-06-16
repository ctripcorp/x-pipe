package com.ctrip.xpipe.redis.meta.server.multidc;


import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.core.meta.DcMetaManager;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultDcMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManager;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.ProxyRedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxy;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxyFactory;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.meta.impl.DefaultCurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.impl.DefaultDcMetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author wenchao.meng
 *
 * Dec 12, 2016
 */
@Component
public class DefaultMultiDcService implements MultiDcService{
	
	private static Logger logger = LoggerFactory.getLogger(DefaultMultiDcService.class);

	@Autowired
	private MetaServerMultiDcServiceManager metaServerMultiDcServiceManager;
	
	@Autowired
	private MetaServerConfig metaServerConfig;

	@Override
	public KeeperMeta getActiveKeeper(String dcName, String clusterId, String shardId) {
		MetaServerMultiDcService metaServerMultiDcService = getMetaServerMultiDcService(dcName);
		if (null == metaServerMultiDcService) return null;

		KeeperMeta keeperMeta = metaServerMultiDcService.getActiveKeeper(clusterId, shardId);
		return keeperMeta;
	}

	@Override
	public ProxyRedisMeta getPeerMaster(String dcName, String clusterId, String shardId) {
		logger.info("[getPeerMaster] .....");
		MetaServerMultiDcService metaServerMultiDcService = getMetaServerMultiDcService(dcName);
		if (null == metaServerMultiDcService) return null;
		RouteMeta meta = getRouteMeta(dcName, clusterId);
		RedisProxy proxy = null;
		if(meta != null) {
			proxy = RedisProxyFactory.create(meta);
		}
		return metaServerMultiDcService.getPeerMaster(clusterId, shardId, proxy);
	}
	@Autowired
	DefaultDcMetaCache dcMetaCache;

	RouteMeta getRouteMeta(String dcName, String clusterId) {
		DcMetaManager dcMetaManager = dcMetaCache.getDcMeta();
		DcMeta meta = dcMetaManager.getDcMeta();
		List<RouteMeta> list = meta.getRoutes();
		ClusterMeta clusterMeta = dcMetaManager.getClusterMeta(clusterId);
		for(RouteMeta m: list) {
			if(m.getDstDc().equals(dcName) && m.getOrgId().equals(clusterMeta.getOrgId())) {
				return m;
			}
		}
		return null;
	}


	private MetaServerMultiDcService getMetaServerMultiDcService(String dcName) {
		dcName = dcName.toLowerCase();
		DcInfo dcInfo = metaServerConfig.getDcInofs().get(dcName);
		if(dcInfo == null){
			logger.error("[getMetaServerMultiDcService][dc info null]{}", dcName);
			return null;
		}

		return metaServerMultiDcServiceManager.getOrCreate(dcInfo.getMetaServerAddress());
	}
}
