package com.ctrip.xpipe.redis.meta.server.impl;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Resource;

import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcCheckMessage;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultCurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.dcchange.ChangePrimaryDcAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.RedisReadonly;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.AtLeastOneChecker;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;

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
		return currentMetaManager.getRedisMaster(clusterId, shardId);
	}

	public void setConfig(MetaServerConfig config) {
		this.config = config;
	}

	// no ClusterMovingMethod
	@Override
	public KeeperMeta getActiveKeeper(String clusterId, String shardId, ForwardInfo forwardInfo) {

		logger.info("[getActiveKeeper]{}, {}", clusterId, shardId);
		return currentMetaManager.getKeeperActive(clusterId, shardId);
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
		dcMetaCache.clusterDeleted(clusterId);
	}

	@Override
	public void updateUpstream(String clusterId, String shardId, String ip, int port, ForwardInfo forwardInfo) {

		if (!dcMetaCache.isCurrentDcPrimary(clusterId, shardId)) {

			logger.info("[updateUpstream]{},{},{},{}", clusterId, shardId, ip, port);
			currentMetaManager.setKeeperMaster(clusterId, shardId, ip, port);
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
			boolean result = new AtLeastOneChecker(redises, keyedObjectPool, scheduled).check();
			if(result){
				return new PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT.SUCCESS);
			}
			return new PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT.FAIL, "all redis dead:" + redises); 
		}
		return new PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT.SUCCESS, String.format("current dc :%s is not new primary: %s ", currentDc, newPrimaryDc));
	}

	@Override
	public void makeMasterReadOnly(String clusterId, String shardId, boolean readOnly, ForwardInfo forwardInfo) {
		
		logger.info("[makeMasterReadOnly]{},{},{}", clusterId, shardId, readOnly);
		
		if(!dcMetaCache.isCurrentDcPrimary(clusterId, shardId)){
			logger.warn("[makeMasterReadOnly]current dc not primary:{}, {}", dcMetaCache.getCurrentDc(), dcMetaCache.getPrimaryDc(clusterId, shardId));
			return ;
		}
		Pair<String, Integer>  keeperMaster = currentMetaManager.getKeeperMaster(clusterId, shardId);
		
		RedisReadonly redisReadOnly = RedisReadonly.create(keeperMaster.getKey(), keeperMaster.getValue(), keyedObjectPool, scheduled); 
		try {
			if(readOnly){
				logger.info("[makeMasterReadOnly][readonly]{}", keeperMaster);
				redisReadOnly.makeReadOnly();
			}else{
				logger.info("[makeMasterReadOnly][writable]{}", keeperMaster);
				redisReadOnly.makeWritable();
			}
		} catch (Exception e) {
			logger.error("[makeMasterReadOnly]" + keeperMaster, e);
		}
	}

	@Override
	public PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc,
			ForwardInfo forwardInfo) {

		logger.info("[doChangePrimaryDc]{}, {}, {}", clusterId, shardId, newPrimaryDc);
		dcMetaCache.primaryDcChanged(clusterId, shardId, newPrimaryDc);
		
		return changePrimaryDcAction.changePrimaryDc(clusterId, shardId, newPrimaryDc);
	}
}
