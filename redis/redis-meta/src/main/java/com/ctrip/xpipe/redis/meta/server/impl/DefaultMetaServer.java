package com.ctrip.xpipe.redis.meta.server.impl;



import java.net.InetSocketAddress;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.cluster.ClusterMovingMethod;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultCurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.ctrip.xpipe.utils.IpUtils;
import com.site.lookup.util.StringUtils;

/**
 * @author marsqing
 *
 *         May 25, 2016 5:24:27 PM
 */
@Component
public class DefaultMetaServer extends DefaultCurrentClusterServer implements MetaServer {
	
	
	@Resource( name = "clientPool" )
	private SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool;
	

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
	public KeeperMeta getActiveKeeper(String clusterId, String shardId) {
		
		return currentMetaServerMeta.getKeeperActive(clusterId, shardId);
	}

	@Override
	public RedisMeta getRedisMaster(String clusterId, String shardId) {
		return currentMetaServerMeta.getRedisMaster(clusterId, shardId);
	}
		
	@Override
	public KeeperMeta getUpstreamKeeper(String clusterId, String shardId) throws Exception {
		
		String address = currentMetaServerMeta.getUpstream(clusterId, shardId);
		logger.info("[getUpstreamKeeper]-{}-", address);
		if(StringUtils.isEmpty(address)){
			return null;
		}
		
		InetSocketAddress inetSocketAddress = IpUtils.parseSingle(address);
		KeeperMeta keeperMeta = new KeeperMeta();
		keeperMeta.setIp(inetSocketAddress.getHostName());
		keeperMeta.setPort(inetSocketAddress.getPort());
		keeperMeta.setActive(true);
		return keeperMeta;
	}

	public void setConfig(MetaServerConfig config) {
		this.config = config;
	}


	@Override
	public int getOrder() {
		return SlotManager.ORDER + 1;
	}

	@ClusterMovingMethod
	@Override
	public ShardStatus getShardStatus(String clusterId, String shardId, ForwardInfo forwardInfo) throws Exception {
		
		return new ShardStatus(getActiveKeeper(clusterId, shardId), getUpstreamKeeper(clusterId, shardId), getRedisMaster(clusterId, shardId));
	}

	@ClusterMovingMethod
	@Override
	public void ping(String clusterId, String shardId, KeeperInstanceMeta keeperInstanceMeta, ForwardInfo forwardInfo) {
		logger.info("[ping]{},{},{},{}", clusterId, shardId, keeperInstanceMeta, forwardInfo);
		
	}
	
	@Override
	protected void doSlotAdd(int slotId) {
		super.doSlotAdd(slotId);
		currentMetaServerMeta.addSlot(slotId);
	}
	
	@Override
	protected void doSlotDelete(int slotId) {
		super.doSlotDelete(slotId);
		
		currentMetaServerMeta.deleteSlot(slotId);;
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
		logger.info("[updateUpstream]{},{},{},{}", clusterId, shardId, ip, port);
	}

}
