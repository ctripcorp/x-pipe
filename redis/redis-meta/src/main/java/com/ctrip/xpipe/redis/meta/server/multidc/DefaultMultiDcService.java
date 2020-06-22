package com.ctrip.xpipe.redis.meta.server.multidc;


import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

		return metaServerMultiDcService.getPeerMaster(clusterId, shardId);
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
