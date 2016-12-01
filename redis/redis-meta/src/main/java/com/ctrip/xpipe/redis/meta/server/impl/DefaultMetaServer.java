package com.ctrip.xpipe.redis.meta.server.impl;

import java.util.List;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcCheckMessage;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultCurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.dcchange.AtLeastOneChecker;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;

/**
 * @author marsqing
 *
 *         May 25, 2016 5:24:27 PM
 */
@Component
public class DefaultMetaServer extends DefaultCurrentClusterServer implements MetaServer {

	@Resource(name = "clientPool")
	private XpipeNettyClientKeyedObjectPool clientPool;

	@SuppressWarnings("unused")
	@Autowired
	private MetaServerConfig config;

	@Autowired
	private CurrentMetaManager currentMetaServerMeta;

	@Autowired
	private DcMetaCache dcMetaCache;

	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();

		LifecycleHelper.initializeIfPossible(currentMetaServerMeta);
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();

		LifecycleHelper.startIfPossible(currentMetaServerMeta);

	}

	@Override
	protected void doStop() throws Exception {

		LifecycleHelper.stopIfPossible(currentMetaServerMeta);
		super.doStop();
	}

	@Override
	protected void doDispose() throws Exception {

		LifecycleHelper.disposeIfPossible(currentMetaServerMeta);
		super.doDispose();
	}

	@Override
	public RedisMeta getRedisMaster(String clusterId, String shardId) {
		return currentMetaServerMeta.getRedisMaster(clusterId, shardId);
	}

	public void setConfig(MetaServerConfig config) {
		this.config = config;
	}

	// no ClusterMovingMethod
	@Override
	public KeeperMeta getActiveKeeper(String clusterId, String shardId, ForwardInfo forwardInfo) {

		logger.info("[getActiveKeeper]{}, {}", clusterId, shardId);
		return currentMetaServerMeta.getKeeperActive(clusterId, shardId);
	}

	@Override
	protected void doSlotAdd(int slotId) {

		super.doSlotAdd(slotId);
		currentMetaServerMeta.addSlot(slotId);
	}

	@Override
	protected void doSlotDelete(int slotId) {
		super.doSlotDelete(slotId);

		currentMetaServerMeta.deleteSlot(slotId);
		;
	}

	@Override
	protected void doSlotExport(int slotId) {
		super.doSlotExport(slotId);
		currentMetaServerMeta.exportSlot(slotId);
	}

	@Override
	protected void doSlotImport(int slotId) {
		super.doSlotImport(slotId);
		currentMetaServerMeta.importSlot(slotId);
	}

	@Override
	public String getCurrentMeta() {
		return currentMetaServerMeta.getCurrentMetaDesc();
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
		dcMetaCache.clusterDeleted(clusterId);
	}

	@Override
	public void updateUpstream(String clusterId, String shardId, String ip, int port, ForwardInfo forwardInfo)
			throws Exception {

		if (!dcMetaCache.isCurrentDcPrimary(clusterId, shardId)) {

			logger.info("[updateUpstream]{},{},{},{}", clusterId, shardId, ip, port);
			currentMetaServerMeta.setKeeperMaster(clusterId, shardId, ip, port);
		} else {
			logger.warn("[updateUpstream][current is primary dc, do not update]{},{},{},{}", clusterId, shardId, ip,
					port);
		}
	}

	@Override
	public PrimaryDcCheckMessage changePrimaryDcCheck(String clusterId, String shardId, String newPrimaryDc,
			ForwardInfo forwardInfo) {
		
		logger.info("[changePrimaryDcCheck]{}, {}, {}, {}", clusterId, shardId, newPrimaryDc, forwardInfo);
		String currentPrimaryDc = dcMetaCache.getPrimaryDc(clusterId, shardId);
		String currentDc = dcMetaCache.getCurrentDc();
		
		if(newPrimaryDc.equalsIgnoreCase(currentPrimaryDc)){
			
			return new PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT.PRIMARY_DC_ALREADY_IS_NEW, String.format("%s already primary dc", newPrimaryDc)); 
		}
		
		if(currentDc.equalsIgnoreCase(newPrimaryDc)){
			
			List<RedisMeta> redises = dcMetaCache.getShardRedises(clusterId, shardId);
			boolean result = new AtLeastOneChecker(redises, clientPool).check();
			if(result){
				return new PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT.SUCCESS);
			}
			return new PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT.FAIL, "all redis dead:" + redises); 
		}
		return new PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT.SUCCESS, String.format("current dc :%s is not new primary: %s ", currentDc, newPrimaryDc));
	}
}
