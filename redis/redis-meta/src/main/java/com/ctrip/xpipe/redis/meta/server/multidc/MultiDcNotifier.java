package com.ctrip.xpipe.redis.meta.server.multidc;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * @author wenchao.meng
 *
 *         Nov 3, 2016
 */
public class MultiDcNotifier implements MetaServerStateChangeHandler {

	protected Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private MetaServerConfig metaServerConfig;

	@Resource( name = AbstractSpringConfigContext.GLOBAL_EXECUTOR )
	private ExecutorService executors;

	@Autowired
	private MetaServerMultiDcServiceManager metaServerMultiDcServiceManager;

	@Autowired
	public DcMetaCache dcMetaCache;

	@Override
	public void keeperActiveElected(Long clusterDbId, Long shardDbId, KeeperMeta activeKeeper) {

		if (!dcMetaCache.isCurrentDcPrimary(clusterDbId, shardDbId)) {
			logger.info("[keeperActiveElected][current dc backup, do nothing]{}, {}, {}", clusterDbId, shardDbId, activeKeeper);
			return;
		}
		
		if(activeKeeper == null){
			return;
		}

		Map<String, DcInfo> dcInfos = metaServerConfig.getDcInofs();
		Set<String> backupDcs = dcMetaCache.getBakupDcs(clusterDbId, shardDbId);
		Pair<String, String> clusterShard = dcMetaCache.clusterShardDbId2Name(clusterDbId, shardDbId);
		logger.info("[keeperActiveElected][current dc primary, notify backup dc]{}:{}, {}:{}, {}, {}", clusterDbId, clusterShard.getKey(),
				shardDbId, clusterShard.getValue(), backupDcs, activeKeeper);
		for (String backupDcName : backupDcs) {

			DcInfo dcInfo = dcInfos.get(backupDcName);

			if (dcInfo == null) {
				logger.error("[keeperActiveElected][backup dc, but can not find dcinfo]{}, {}", backupDcName, dcInfos);
				continue;
			}
			MetaServerMultiDcService metaServerMultiDcService = metaServerMultiDcServiceManager
					.getOrCreate(dcInfo.getMetaServerAddress());
			executors.execute(new BackupDcNotifyTask(metaServerMultiDcService, clusterShard.getKey(), clusterShard.getValue(), activeKeeper));
		}

	}

	@Override
	public void currentMasterChanged(Long clusterDbId, Long shardDbId) {
		// notify remote dc for local peer master change
		Map<String, DcInfo> dcInfos = metaServerConfig.getDcInofs();
		Set<String> relatedDcs = dcMetaCache.getRelatedDcs(clusterDbId, shardDbId);
		String currentDc = dcMetaCache.getCurrentDc();
		Pair<String, String> clusterShard = dcMetaCache.clusterShardDbId2Name(clusterDbId, shardDbId);

		logger.info("[peerMasterChanged][notify related dc]{}:{}, {}:{}, {}", clusterDbId, clusterShard.getKey(),
				shardDbId, clusterShard.getValue(), relatedDcs);
		for (String dcId : relatedDcs) {
			if (currentDc.equalsIgnoreCase(dcId) || StringUtil.isEmpty(dcId)) {
				continue;
			}
			DcInfo dcInfo = dcInfos.get(dcId);

			if (dcInfo == null) {
				logger.error("[peerMasterChanged][can not find dcinfo]{}, {}", dcId, dcInfos);
				continue;
			}
			MetaServerMultiDcService metaServerMultiDcService = metaServerMultiDcServiceManager
					.getOrCreate(dcInfo.getMetaServerAddress());
			executors.execute(new PeerDcNotifyTask(metaServerMultiDcService, currentDc, clusterShard.getKey(), clusterShard.getValue()));
		}
	}

	public class BackupDcNotifyTask extends AbstractExceptionLogTask {

		private MetaServerMultiDcService metaServerMultiDcService;

		private String clusterId;

		private String shardId;

		private KeeperMeta activeKeeper;

		public BackupDcNotifyTask(MetaServerMultiDcService metaServerMultiDcService, String clusterId, String shardId,
				KeeperMeta activeKeeper) {
			this.metaServerMultiDcService = metaServerMultiDcService;
			this.clusterId = clusterId;
			this.shardId = shardId;
			this.activeKeeper = activeKeeper;
		}

		@Override
		protected void doRun() throws Exception {

			logger.info("[doRun]{}, {}, {}, {}, {}", metaServerMultiDcService, clusterId, shardId, activeKeeper);
			metaServerMultiDcService.upstreamChange(clusterId, shardId, activeKeeper.getIp(), activeKeeper.getPort());

		}

	}

	public class PeerDcNotifyTask extends AbstractExceptionLogTask {

		private MetaServerMultiDcService metaServerMultiDcService;

		private String dcId;

		private String clusterId;

		private String shardId;

		public PeerDcNotifyTask(MetaServerMultiDcService metaServerMultiDcService, String dcId, String clusterId, String shardId) {
			this.metaServerMultiDcService = metaServerMultiDcService;
			this.dcId = dcId;
			this.clusterId = clusterId;
			this.shardId = shardId;
		}

		@Override
		protected void doRun() throws Exception {

			logger.info("[doRun]{}, {}, {}", metaServerMultiDcService, clusterId, shardId);
			metaServerMultiDcService.upstreamPeerChange(dcId, clusterId, shardId);

		}
	}

}
