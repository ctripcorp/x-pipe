package com.ctrip.xpipe.redis.meta.server.multidc;


import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
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

	@Autowired
	private DcMetaCache dcMetaCache;

	@Override
	public KeeperMeta getActiveKeeper(String dcName, Long clusterDbId, Long shardDbId) {
		MetaServerMultiDcService metaServerMultiDcService = getMetaServerMultiDcService(dcName);
		if (null == metaServerMultiDcService) return null;

		Pair<String, String> clusterShard = dcMetaCache.clusterShardDbId2Name(clusterDbId, shardDbId);
		return metaServerMultiDcService.getActiveKeeper(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public RedisMeta getPeerMaster(String dcName, Long clusterDbId, Long shardDbId) {
		MetaServerMultiDcService metaServerMultiDcService = getMetaServerMultiDcService(dcName);
		if (null == metaServerMultiDcService) return null;

		Pair<String, String> clusterShard = dcMetaCache.clusterShardDbId2Name(clusterDbId, shardDbId);
		return metaServerMultiDcService.getPeerMaster(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public String getSids(String dcName, String srcDc, Long clusterDbId, Long shardDbId) {

		MetaServerMultiDcService metaServerMultiDcService = getMetaServerMultiDcService(dcName);
		if (null == metaServerMultiDcService) return null;

		Pair<String, String> clusterShard = dcMetaCache.clusterShardDbId2Name(clusterDbId, shardDbId);
		return metaServerMultiDcService.getSids(srcDc, clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public void sidsChange(String dcName, Long clusterDbId, Long shardDbId, String sids) {

		MetaServerMultiDcService metaServerMultiDcService = getMetaServerMultiDcService(dcName);
		if (null == metaServerMultiDcService) return;

		Pair<String, String> clusterShard = dcMetaCache.clusterShardDbId2Name(clusterDbId, shardDbId);
		metaServerMultiDcService.sidsChange(clusterShard.getKey(), clusterShard.getValue(), sids);
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
