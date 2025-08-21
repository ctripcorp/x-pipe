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
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

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

		boolean primary = dcMetaCache.isCurrentDcPrimary(clusterDbId, shardDbId);
		logger.info("[keeperActiveElected][current dc]primary={}, cluster_{}, shard_{}, {}", primary, clusterDbId, shardDbId, activeKeeper);

		if (primary) {
			keeperActiveElectedNotifyDcs(clusterDbId, shardDbId, activeKeeper, dcMetaCache.getBakupDcs(clusterDbId, shardDbId));
		}

		keeperActiveElectedNotifyDcs(clusterDbId, shardDbId, activeKeeper, dcMetaCache.getDownstreamDcs(dcMetaCache.getCurrentDc(), clusterDbId, shardDbId));
	}

	private void keeperActiveElectedNotifyDcs(Long clusterDbId, Long shardDbId, KeeperMeta activeKeeper, Set<String> dcs) {
		if(activeKeeper == null){
			return;
		}

		Map<String, DcInfo> dcInfos = metaServerConfig.getDcInofs();
		Pair<String, String> clusterShard = dcMetaCache.clusterShardDbId2Name(clusterDbId, shardDbId);

		logger.info("[keeperActiveElected][notify dcs]{}:{}, {}:{}, {}, {}", clusterDbId, clusterShard.getKey(),
				shardDbId, clusterShard.getValue(), dcs, activeKeeper);

		executeKeeperActiveElectedNotifyTask(dcs, activeKeeper, dcInfos, clusterShard);
	}

	private void executeKeeperActiveElectedNotifyTask(Set<String> dcs, KeeperMeta activeKeeper, Map<String, DcInfo> dcInfos, Pair<String, String> clusterShard) {

		for (String dc : dcs) {

			DcInfo dcInfo = dcInfos.get(dc);

			if (dcInfo == null) {
				logger.error("[keeperActiveElected][can not find dcinfo]{}, {}", dc, dcInfos);
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

			logger.info("[doRun]{}, {}, {}, {}", metaServerMultiDcService, clusterId, shardId, activeKeeper);
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
