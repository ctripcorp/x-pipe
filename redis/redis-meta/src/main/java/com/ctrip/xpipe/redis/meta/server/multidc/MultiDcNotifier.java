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
	public void keeperActiveElected(String clusterId, String shardId, KeeperMeta activeKeeper) {

		if (!dcMetaCache.isCurrentDcPrimary(clusterId, shardId)) {
			logger.info("[keeperActiveElected][current dc backup, do nothing]{}, {}", clusterId, shardId, activeKeeper);
			return;
		}
		
		if(activeKeeper == null){
			return;
		}

		Map<String, DcInfo> dcInfos = metaServerConfig.getDcInofs();
		Set<String> backupDcs = dcMetaCache.getBakupDcs(clusterId, shardId);
		logger.info("[keeperActiveElected][current dc primary, notify backup dc]{}, {}, {}, {}", clusterId, shardId,
				backupDcs, activeKeeper);
		for (String backupDcName : backupDcs) {

			DcInfo dcInfo = dcInfos.get(backupDcName);

			if (dcInfo == null) {
				logger.error("[keeperActiveElected][backup dc, but can not find dcinfo]{}, {}", backupDcName, dcInfos);
				continue;
			}
			MetaServerMultiDcService metaServerMultiDcService = metaServerMultiDcServiceManager
					.getOrCreate(dcInfo.getMetaServerAddress());
			executors.execute(new BackupDcNotifyTask(metaServerMultiDcService, clusterId, shardId, activeKeeper));
		}

	}

	@Override
	public void keeperMasterChanged(String clusterId, String shardId, Pair<String, Integer> newMaster) {

	}

	@Override
	public void peerMasterChanged(String clusterId, String shardId) {
		Map<String, DcInfo> dcInfos = metaServerConfig.getDcInofs();
		Set<String> relatedDcs = dcMetaCache.getRelatedDcs(clusterId, shardId);
		logger.info("[peerMasterChanged][notify related dc]{}, {}, {}", clusterId, shardId, relatedDcs);
		for (String dcName : relatedDcs) {
			if (dcMetaCache.getCurrentDc().equalsIgnoreCase(dcName)) {
				continue;
			}
			DcInfo dcInfo = dcInfos.get(dcName);

			if (dcInfo == null) {
				logger.error("[peerMasterChanged][can not find dcinfo]{}, {}", dcName, dcInfos);
				continue;
			}
			MetaServerMultiDcService metaServerMultiDcService = metaServerMultiDcServiceManager
					.getOrCreate(dcInfo.getMetaServerAddress());
			executors.execute(new PeerDcNotifyTask(metaServerMultiDcService, dcName, clusterId, shardId));
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
