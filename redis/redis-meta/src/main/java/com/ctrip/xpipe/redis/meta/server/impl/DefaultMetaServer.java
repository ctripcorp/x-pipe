package com.ctrip.xpipe.redis.meta.server.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.exception.SIMPLE_RETURN_CODE;
import com.ctrip.xpipe.exception.SimpleErrorMessage;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcCheckMessage;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultCurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.crdt.master.PeerMasterChooseAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.ChangePrimaryDcAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.PrimaryDcPrepareToChange;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.AtLeastOneChecker;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author marsqing
 *
 *         May 25, 2016 5:24:27 PM
 */
@Component
public class DefaultMetaServer extends DefaultCurrentClusterServer implements MetaServer {

	@Resource(name = MetaServerContextConfig.CLIENT_POOL)
	private XpipeNettyClientKeyedObjectPool keyedObjectPool;

	@Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;

	@SuppressWarnings("unused")
	@Autowired
	private MetaServerConfig config;

	@Autowired
	private CurrentMetaManager currentMetaManager;

	@Autowired
	private DcMetaCache dcMetaCache;
	
	@Autowired
	private ChangePrimaryDcAction  changePrimaryDcAction;

	@Autowired
	private PrimaryDcPrepareToChange primaryDcPrepareToChange;

	@Autowired
	private PeerMasterChooseAction peerMasterChooseAction;

	@Autowired
	private MultiDcService multiDcService;

	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();

		LifecycleHelper.initializeIfPossible(currentMetaManager);
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();

		LifecycleHelper.startIfPossible(currentMetaManager);

	}

	@Override
	protected void doStop() throws Exception {

		LifecycleHelper.stopIfPossible(currentMetaManager);
		super.doStop();
	}

	@Override
	protected void doDispose() throws Exception {

		LifecycleHelper.disposeIfPossible(currentMetaManager);
		super.doDispose();
	}

	@Override
	public RedisMeta getRedisMaster(String clusterId, String shardId) {
		Pair<Long, Long> clusterShard = dcMetaCache.clusterShardId2DbId(clusterId, shardId);
		return currentMetaManager.getRedisMaster(clusterShard.getKey(), clusterShard.getValue());
	}

	public void setConfig(MetaServerConfig config) {
		this.config = config;
	}

	// no ClusterMovingMethod
	@Override
	public KeeperMeta getActiveKeeper(String clusterId, String shardId, ForwardInfo forwardInfo) {

		logger.debug("[getActiveKeeper]{}, {}", clusterId, shardId);
		Pair<Long, Long> clusterShard = dcMetaCache.clusterShardId2DbId(clusterId, shardId);
		return currentMetaManager.getKeeperActive(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public RedisMeta getCurrentCRDTMaster(String clusterId, String shardId, ForwardInfo forwardInfo) {
		logger.debug("[getCurrentCRDTMaster]{}, {}", clusterId, shardId);
		Pair<Long, Long> clusterShard = dcMetaCache.clusterShardId2DbId(clusterId, shardId);
		return currentMetaManager.getCurrentCRDTMaster(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public String getSids(String srcDcId, String clusterId, String shardId, ForwardInfo forwardInfo) {

		Pair<Long, Long> clusterShard = dcMetaCache.clusterShardId2DbId(clusterId, shardId);

		if (!srcDcId.equalsIgnoreCase(dcMetaCache.getCurrentDc())) {
			String upstreamDc = dcMetaCache.getUpstreamDc(dcMetaCache.getCurrentDc(), clusterShard.getKey(), clusterShard.getValue());
			logger.debug("[getSids] from upstream dc {}, srcDc {}, {}, {}", upstreamDc, srcDcId, clusterId, shardId);
			return multiDcService.getSids(upstreamDc, srcDcId, clusterShard.getKey(), clusterShard.getValue());
		}

		logger.debug("[getSids]{}, {}", clusterId, shardId);

		return currentMetaManager.getSids(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public void sidsChange(String clusterId, String shardId, String sids, ForwardInfo forwardInfo) {

		Pair<Long, Long> clusterShard = dcMetaCache.clusterShardId2DbId(clusterId, shardId);

		ApplierMeta applier = currentMetaManager.getApplierActive(clusterShard.getKey(), clusterShard.getValue());
		if (applier != null) {
			Pair<String, Integer> applierMaster = currentMetaManager.getApplierMaster(clusterShard.getKey(), clusterShard.getValue());

			logger.info("[sidsChange][applier]{},{},{},{},{}", clusterId, shardId, applierMaster.getKey(), applierMaster.getValue(), sids);
			currentMetaManager.setApplierMasterAndNotify(clusterShard.getKey(), clusterShard.getValue(), applierMaster.getKey(), applierMaster.getValue(), sids);
		}

		Set<String> downstreamDcs = dcMetaCache.getDownstreamDcs(dcMetaCache.getCurrentDc(), clusterShard.getKey(), clusterShard.getValue());
		for (String downstreamDc : downstreamDcs) {
			logger.debug("[sidsChange]downstream dc {}, cluster_{}, shard_{}", downstreamDc, clusterId, shardId);
			multiDcService.sidsChange(downstreamDc, clusterShard.getKey(), clusterShard.getValue(), sids);
		}
	}

	@Override
	public RedisMeta getCurrentMaster(String clusterId, String shardId, ForwardInfo forwardInfo) {
		logger.debug("[getCurrentMaster]{}, {}", clusterId, shardId);
		Pair<Long, Long> clusterShard = dcMetaCache.clusterShardId2DbId(clusterId, shardId);
		return currentMetaManager.getCurrentMaster(clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	protected void doSlotAdd(int slotId) {

		super.doSlotAdd(slotId);
		currentMetaManager.addSlot(slotId);
	}

	@Override
	protected void doSlotDelete(int slotId) {
		super.doSlotDelete(slotId);

		currentMetaManager.deleteSlot(slotId);
	}

	@Override
	protected void doSlotExport(int slotId) {
		super.doSlotExport(slotId);
		currentMetaManager.exportSlot(slotId);
	}

	@Override
	protected void doSlotImport(int slotId) {
		super.doSlotImport(slotId);
		currentMetaManager.importSlot(slotId);
	}

	@Override
	public String getCurrentMeta() {
		return currentMetaManager.getCurrentMetaDesc();
	}

	@Override
	public void clusterAdded(ClusterMeta clusterMeta, ForwardInfo forwardInfo) {
		logger.info("[clusterAdded]{}", clusterMeta);
		dcMetaCache.clusterAdded(clusterMeta);
	}

	@Override
	public void clusterModified(ClusterMeta clusterMeta, ForwardInfo forwardInfo) {
		logger.info("[clusterModified]{}", clusterMeta);
		dcMetaCache.clusterModified(clusterMeta);

	}

	@Override
	public void clusterDeleted(String clusterId, ForwardInfo forwardInfo) {

		logger.info("[clusterDeleted]{}", clusterId);
		dcMetaCache.clusterDeleted(dcMetaCache.clusterId2DbId(clusterId));
	}

	@Override
	public void updateUpstream(String clusterId, String shardId, String ip, int port, ForwardInfo forwardInfo) {

		Pair<Long, Long> clusterShard = dcMetaCache.clusterShardId2DbId(clusterId, shardId);

		if (dcMetaCache.isCurrentShardParentCluster(clusterShard.getKey(), clusterShard.getValue())) {
		    if (dcMetaCache.isCurrentDcPrimary(clusterShard.getKey(), clusterShard.getValue())) {
				logger.warn("[updateUpstream][current is primary dc, do not update]{},{},{},{}", clusterShard.getKey(), clusterShard.getValue(), ip,
						port);
			} else {
				logger.info("[updateUpstream]{},{},{},{}", clusterId, shardId, ip, port);
				currentMetaManager.setKeeperMaster(clusterShard.getKey(), clusterShard.getValue(), ip, port);
			}
		} else {
			List<KeeperMeta> keepers = dcMetaCache.getShardKeepers(clusterShard.getKey(), clusterShard.getValue());
			if (!CollectionUtils.isEmpty(keepers)) {
				logger.info("[hetero][keeper][updateUpstream]{},{},{},{}", clusterId, shardId, ip, port);
				currentMetaManager.setKeeperMaster(clusterShard.getKey(), clusterShard.getValue(), ip, port);
				return;
			}

			List<ApplierMeta> appliers = dcMetaCache.getShardAppliers(clusterShard.getKey(), clusterShard.getValue());
			if (!CollectionUtils.isEmpty(appliers)) {
				logger.info("[hetero][applier][updateUpstream]{},{},{},{}", clusterId, shardId, ip, port);
				String sid = currentMetaManager.getSrcSids(clusterShard.getKey(), clusterShard.getValue());
				currentMetaManager.setApplierMasterAndNotify(clusterShard.getKey(), clusterShard.getValue(), ip, port, sid);
			}
		}
	}

	@Override
	public void upstreamPeerChange(String upstreamDcId, String clusterId, String shardId, ForwardInfo forwardInfo) {
		Pair<Long, Long> clusterShard = dcMetaCache.clusterShardId2DbId(clusterId, shardId);
		ClusterMeta clusterMeta = dcMetaCache.getClusterMeta(clusterShard.getKey());
		if (null == clusterMeta || !ClusterType.lookup(clusterMeta.getType()).supportMultiActiveDC()) {
			logger.info("[upstreamPeerChange] cluster {} not found or not support", clusterId);
			return;
		}

		peerMasterChooseAction.choosePeerMaster(upstreamDcId, clusterShard.getKey(), clusterShard.getValue());
	}

	@Override
	public PrimaryDcCheckMessage changePrimaryDcCheck(String clusterId, String shardId, String newPrimaryDc,
			ForwardInfo forwardInfo) {

		logger.info("[changePrimaryDcCheck]{}, {}, {}, {}", clusterId, shardId, newPrimaryDc, forwardInfo);
		Pair<Long, Long> clusterShard = dcMetaCache.clusterShardId2DbId(clusterId, shardId);
		String currentPrimaryDc = dcMetaCache.getPrimaryDc(clusterShard.getKey(), clusterShard.getValue());
		String currentDc = dcMetaCache.getCurrentDc();

		if(newPrimaryDc.equalsIgnoreCase(currentPrimaryDc)){

			return new PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT.PRIMARY_DC_ALREADY_IS_NEW, String.format("%s already primary dc", newPrimaryDc));
		}

		if(currentDc.equalsIgnoreCase(newPrimaryDc)){

			List<RedisMeta> redises = dcMetaCache.getShardRedises(clusterShard.getKey(), clusterShard.getValue());
			SimpleErrorMessage result = new AtLeastOneChecker(redises, keyedObjectPool, scheduled).check();
			if(result.getErrorType() == SIMPLE_RETURN_CODE.SUCCESS){
				return new PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT.SUCCESS);
			}
			return new PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT.FAIL, "all redises dead:" + result.getErrorMessage());
		}
		return new PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT.SUCCESS, String.format("current dc :%s is not new primary: %s ", currentDc, newPrimaryDc));
	}

	@Override
	public MetaServerConsoleService.PreviousPrimaryDcMessage makeMasterReadOnly(String clusterId, String shardId, boolean readOnly, ForwardInfo forwardInfo) {

		logger.info("[makeMasterReadOnly]{},{},{}", clusterId, shardId, readOnly);
		Pair<Long, Long> clusterShard = dcMetaCache.clusterShardId2DbId(clusterId, shardId);
		if(!dcMetaCache.isCurrentDcPrimary(clusterShard.getKey(), clusterShard.getValue())){
			logger.warn("[makeMasterReadOnly]current dc not primary:{}, {}", dcMetaCache.getCurrentDc(), dcMetaCache.getPrimaryDc(clusterShard.getKey(), clusterShard.getValue()));
			return null;
		}


		MetaServerConsoleService.PreviousPrimaryDcMessage message = null;
		if(readOnly){
			message = primaryDcPrepareToChange.prepare(clusterShard.getKey(), clusterShard.getValue());
		}else {
			message = primaryDcPrepareToChange.deprepare(clusterShard.getKey(), clusterShard.getValue());
		}
		return message;
	}

	@Override
	public PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc, MetaServerConsoleService.PrimaryDcChangeRequest request,
			ForwardInfo forwardInfo) {
		Pair<Long, Long> clusterShard = dcMetaCache.clusterShardId2DbId(clusterId, shardId);
		ClusterType clusterType = dcMetaCache.getClusterType(clusterShard.getKey());
		if (!clusterType.supportMigration()) {
			logger.info("[doChangePrimaryDc] cluster {} type {} not support migration", clusterId, clusterType);
			return new PrimaryDcChangeMessage(MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT.FAIL,
					"cluster " + clusterId + " not support miggration");
		}

		logger.info("[doChangePrimaryDc]{}, {}, {}, {}", clusterId, shardId, newPrimaryDc, request);
		dcMetaCache.primaryDcChanged(clusterShard.getKey(), clusterShard.getValue(), newPrimaryDc);

		MasterInfo masterInfo = null;
		if(request != null){
			masterInfo = request.getMasterInfo();
		}
		return changePrimaryDcAction.changePrimaryDc(clusterShard.getKey(), clusterShard.getValue(), newPrimaryDc, masterInfo);
	}
}
