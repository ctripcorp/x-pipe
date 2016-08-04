package com.ctrip.xpipe.redis.meta.server.impl;

import java.net.InetSocketAddress;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.cluster.ShardArrange;
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
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultCurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.dao.MetaServerDao;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.ctrip.xpipe.redis.meta.server.service.MetaServerService;
import com.ctrip.xpipe.utils.IpUtils;

/**
 * @author marsqing
 *
 *         May 25, 2016 5:24:27 PM
 */
@Component
public class DefaultMetaServer extends DefaultCurrentClusterServer implements MetaServer {
	
	@Autowired
	private ShardArrange<String>  shardarrange;
	
	@Resource( name = "clientPool" )
	private SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool;
	
	@Autowired
	private MetaServerDao metaServerDao;

	@SuppressWarnings("unused")
	@Autowired
	private MetaServerConfig config;
		
	@Autowired
	private KeeperElectorManager keeperElectorManager;
	
	@Autowired
	private MetaServerService metaServerService;
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		
		LifecycleHelper.initializeIfPossible(metaServerDao);
		LifecycleHelper.initializeIfPossible(keeperElectorManager);
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();
		
		LifecycleHelper.startIfPossible(metaServerDao);
		LifecycleHelper.startIfPossible(keeperElectorManager);
		
		initFromDao();
	}
	
	private void initFromDao() throws Exception {
		
		for(String clusterId : metaServerDao.getClusters()){
			
			logger.info("[initFromDao]{}", clusterId);
			
			ClusterMeta clusterMeta = metaServerDao.getClusterMeta(clusterId);
			if(shardarrange.responsableFor(clusterId)){
				keeperElectorManager.observeCluster(clusterMeta);
			}
		}
	}

	@Override
	protected void doStop() throws Exception {
		
		
		LifecycleHelper.stopIfPossible(keeperElectorManager);
		LifecycleHelper.stopIfPossible(metaServerDao);
		
		super.doStop();
	}
	
	@Override
	protected void doDispose() throws Exception {
		
		LifecycleHelper.disposeIfPossible(keeperElectorManager);
		LifecycleHelper.disposeIfPossible(metaServerDao);
		
		super.doDispose();
	}

	@Override
	public KeeperMeta getActiveKeeper(String clusterId, String shardId) {
		return metaServerDao.getKeeperActive(clusterId, shardId);
	}

	@Override
	public RedisMeta getRedisMaster(String clusterId, String shardId) {
		return metaServerDao.getRedisMaster(clusterId, shardId);
	}
		
	@Override
	public KeeperMeta getUpstreamKeeper(String clusterId, String shardId) throws Exception {
		
		String address = metaServerDao.getUpstream(clusterId, shardId);
		if(address == null){
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
	public void promoteRedisMaster(String clusterId, String shardId, String promoteIp, int promotePort) throws Exception {
		
		RedisMeta redisMaster = new RedisMeta();
		redisMaster.setIp(promoteIp);
		redisMaster.setPort(promotePort);
		metaServerService.updateRedisMaster(clusterId, shardId, redisMaster);
	}

	@Override
	public ShardStatus getShardStatus(String clusterId, String shardId) throws Exception {
		
		return new ShardStatus(getActiveKeeper(clusterId, shardId), getUpstreamKeeper(clusterId, shardId), getRedisMaster(clusterId, shardId));
	}

	@Override
	public void updateActiveKeeper(String clusterId, String shardId, KeeperMeta keeper) throws Exception {
		
		metaServerService.updateKeeperActive(clusterId, shardId, keeper);
	}

	@Override
	public void updateUpstream(String clusterId, String shardId, String upstream) throws Exception {
		metaServerService.updateUpstreamKeeper(clusterId, shardId, upstream);
	}

	@Override
	public int getOrder() {
		return 0;
	}

	@ClusterMovingMethod
	@Override
	public void ping(String clusterId, String shardId, KeeperInstanceMeta keeperInstanceMeta, ForwardInfo forwardInfo) {
		logger.info("[ping]{},{},{},{}", clusterId, shardId, keeperInstanceMeta, forwardInfo);
		
	}
}
