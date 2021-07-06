package com.ctrip.xpipe.redis.meta.server.multidc;


import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.core.meta.DcMetaManager;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.core.util.OrgUtil;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.impl.DefaultDcMetaCache;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
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
	public RedisMeta getPeerMaster(String dcName, String clusterId, String shardId) {
		MetaServerMultiDcService metaServerMultiDcService = getMetaServerMultiDcService(dcName);
		if (null == metaServerMultiDcService) return null;
		RedisMeta redisMeta =  metaServerMultiDcService.getPeerMaster(clusterId, shardId);
		return redisMeta;
	}

	@Autowired
	DefaultDcMetaCache dcMetaCache;

	RouteMeta getRouteMetaByHash(List<RouteMeta> routes, String clusterId) {
		int index = clusterId.hashCode() % (routes.size());
		return routes.get(index);
	}

	@Override
	public RouteMeta getRouteMeta(String dcName, String clusterId) {
		DcMetaManager dcMetaManager = dcMetaCache.getDcMeta();
		DcMeta meta = dcMetaManager.getDcMeta();
		List<RouteMeta> list = meta.getRoutes();
		ClusterMeta clusterMeta = dcMetaManager.getClusterMeta(clusterId);
		List<RouteMeta> routes = new LinkedList<>();
		for(RouteMeta routeMeta: list) {
			if(routeMeta.getDstDc().equals(dcName) && ObjectUtils.equals(routeMeta.getOrgId(), clusterMeta.getOrgId()) ) {
				routes.add(routeMeta);
			}
		}
		if(!routes.isEmpty()) {
			return getRouteMetaByHash(routes, clusterId);
		}
		for(RouteMeta routeMeta: list) {
			if(routeMeta.getDstDc().equals(dcName) && OrgUtil.isDefaultOrg(routeMeta.getOrgId())) {
				routes.add(routeMeta);
			}
		}
		if(!routes.isEmpty()) {
			return getRouteMetaByHash(routes, clusterId);
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
