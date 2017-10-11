package com.ctrip.xpipe.redis.meta.server.impl;

import com.ctrip.xpipe.exception.SIMPLE_RETURN_CODE;
import com.ctrip.xpipe.exception.SimpleErrorMessage;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
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
import com.ctrip.xpipe.redis.meta.server.dcchange.ChangePrimaryDcAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.PrimaryDcPrepareToChange;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.AtLeastOneChecker;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
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
		
		if(!dcMetaCache.isCurrentDcPrimary(clusterId, shardId)){
			logger.warn("[makeMasterReadOnly]current dc not primary:{}, {}", dcMetaCache.getCurrentDc(), dcMetaCache.getPrimaryDc(clusterId, shardId));
			return null;
		}


		MetaServerConsoleService.PreviousPrimaryDcMessage message = null;
		if(readOnly){
			message = primaryDcPrepareToChange.prepare(clusterId, shardId);
		}else {
			message = primaryDcPrepareToChange.deprepare(clusterId, shardId);
		}
		return message;
	}

	@Override
	public PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc, MetaServerConsoleService.PrimaryDcChangeRequest request,
			ForwardInfo forwardInfo) {

		logger.info("[doChangePrimaryDc]{}, {}, {}, {}", clusterId, shardId, newPrimaryDc, request);
		dcMetaCache.primaryDcChanged(clusterId, shardId, newPrimaryDc);

		MasterInfo masterInfo = null;
		if(request != null){
			masterInfo = request.getMasterInfo();
		}
		return changePrimaryDcAction.changePrimaryDc(clusterId, shardId, newPrimaryDc, masterInfo);
	}
}
