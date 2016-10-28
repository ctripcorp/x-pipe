package com.ctrip.xpipe.redis.meta.server.keeper;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJob;
import com.ctrip.xpipe.redis.meta.server.job.XSlaveofJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author wenchao.meng
 *
 * Jul 8, 2016
 */
@Component
public class DefaultKeeperStateChangeHandler extends AbstractLifecycleObservable implements MetaServerStateChangeHandler{
	
	protected static Logger logger = LoggerFactory.getLogger(DefaultKeeperStateChangeHandler.class);
	
	@Resource( name = "clientPool" )
	private SimpleKeyedObjectPool<InetSocketAddress, NettyClient> clientPool;
	
	private ExecutorService executors = Executors.newCachedThreadPool(XpipeThreadFactory.create("DefaultKeeperStateChangeHandler"));
	
	@Autowired
	private DcMetaCache dcMetaCache;
	
	@Autowired
	private CurrentMetaManager  currentMetaServerMetaManager;
	
	@Override
	public void keeperMasterChanged(String clusterId, String shardId, InetSocketAddress newMaster) {
		
		logger.info("[keeperMasterChanged]{},{},{}", clusterId, shardId, newMaster);
		KeeperMeta activeKeeper = currentMetaServerMetaManager.getKeeperActive(clusterId, shardId);
		
		if(activeKeeper == null){
			logger.info("[keeperMasterChanged][no active keeper, do nothing]{},{},{}", clusterId, shardId, newMaster);
			return;
		}
		if(!activeKeeper.isActive()){
			throw new IllegalStateException("[active keeper not active]{}" + activeKeeper);
		}
		
		logger.info("[keeperMasterChanged][set active keeper master]{}, {}", activeKeeper, newMaster);
	
		List<KeeperMeta> keepers = new LinkedList<>();
		keepers.add(activeKeeper);
		new KeeperStateChangeJob(keepers, newMaster, clientPool).execute(executors);
	}


	@Override
	public void keeperActiveElected(String clusterId, String shardId, KeeperMeta activeKeeper) throws Exception {
		
		logger.info("[keeperActiveElected]{},{},{}", clusterId, shardId, activeKeeper);
		
		List<KeeperMeta> keepers = currentMetaServerMetaManager.getSurviveKeepers(clusterId, shardId);
		if(keepers == null || keepers.size() == 0){
			logger.info("[keeperActiveElected][none keeper survive, do nothing]");
			return;
		}
		InetSocketAddress activeKeeperMaster = currentMetaServerMetaManager.getKeeperMaster(clusterId, shardId);
		new KeeperStateChangeJob(keepers, activeKeeperMaster, clientPool).execute(executors);
		
		if(!dcMetaCache.isCurrentDcPrimary(clusterId, shardId)){
			List<RedisMeta> slaves = dcMetaCache.getShardRedises(clusterId, shardId);
			logger.info("[keeperActiveElected][current dc backup, set slave to new keeper]{},{}", clusterId, shardId, slaves);
			new XSlaveofJob(slaves, activeKeeper.getIp(), activeKeeper.getPort(), clientPool).execute(executors);
		}
	}
}
