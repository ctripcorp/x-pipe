package com.ctrip.xpipe.redis.meta.server.keeper;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.redis.meta.server.job.DefaultSlaveOfJob;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 *         Jul 8, 2016
 */
@Component
public class DefaultKeeperStateChangeHandler extends AbstractLifecycle implements MetaServerStateChangeHandler, TopElement {

	protected static Logger logger = LoggerFactory.getLogger(DefaultKeeperStateChangeHandler.class);

	@Resource(name = MetaServerContextConfig.CLIENT_POOL)
	private SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool;

	@Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;

	private ExecutorService executors;

	private KeyedOneThreadTaskExecutor<Pair<String, String>> keyedOneThreadTaskExecutor;

	@Autowired
	private DcMetaCache dcMetaCache;

	@Autowired
	private CurrentMetaManager currentMetaManager;
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		executors = DefaultExecutorFactory.createAllowCoreTimeout("KeeperStateChangeHandler", OsUtils.defaultMaxCoreThreadCount()).createExecutorService();
		keyedOneThreadTaskExecutor = new KeyedOneThreadTaskExecutor<>(executors);
	}
	
	@Override
	public void keeperMasterChanged(String clusterId, String shardId, Pair<String, Integer> newMaster) {

		logger.info("[keeperMasterChanged]{},{},{}", clusterId, shardId, newMaster);
		KeeperMeta activeKeeper = currentMetaManager.getKeeperActive(clusterId, shardId);

		if (activeKeeper == null) {
			logger.info("[keeperMasterChanged][no active keeper, do nothing]{},{},{}", clusterId, shardId, newMaster);
			return;
		}
		if (!activeKeeper.isActive()) {
			throw new IllegalStateException("[active keeper not active]{}" + activeKeeper);
		}

		logger.info("[keeperMasterChanged][set active keeper master]{}, {}", activeKeeper, newMaster);

		List<KeeperMeta> keepers = new LinkedList<>();
		keepers.add(activeKeeper);
		
		keyedOneThreadTaskExecutor.execute(
				new Pair<String, String>(clusterId, shardId),
				createKeeperStateChangeJob(clusterId, keepers, newMaster));
	}

	private KeeperStateChangeJob createKeeperStateChangeJob(String clusterId, List<KeeperMeta> keepers, Pair<String, Integer> master) {

		RouteMeta routeMeta = currentMetaManager.randomRoute(clusterId);
		return new KeeperStateChangeJob(keepers, master, routeMeta, clientPool, scheduled, executors);
	}

	@Override
	public void keeperActiveElected(String clusterId, String shardId, KeeperMeta activeKeeper) {

		logger.info("[keeperActiveElected]{},{},{}", clusterId, shardId, activeKeeper);

		List<KeeperMeta> keepers = currentMetaManager.getSurviveKeepers(clusterId, shardId);
		if (keepers == null || keepers.size() == 0) {
			logger.info("[keeperActiveElected][none keeper survive, do nothing]");
			return;
		}
		Pair<String, Integer> activeKeeperMaster = currentMetaManager.getKeeperMaster(clusterId, shardId);

		KeeperStateChangeJob keeperStateChangeJob = createKeeperStateChangeJob(clusterId, keepers, activeKeeperMaster);

		if (!dcMetaCache.isCurrentDcPrimary(clusterId, shardId)) {
			List<RedisMeta> slaves = dcMetaCache.getShardRedises(clusterId, shardId);
			logger.info("[keeperActiveElected][current dc backup, set slave to new keeper]{},{},{}", clusterId, shardId,
					slaves);
			keeperStateChangeJob.setActiveSuccessCommand(new DefaultSlaveOfJob(slaves, activeKeeper.getIp(), activeKeeper.getPort(), clientPool, scheduled, executors));
		}
		
		keyedOneThreadTaskExecutor.execute(new Pair<String, String>(clusterId, shardId), keeperStateChangeJob);
	}

	@Override
	protected void doDispose() throws Exception {
		
		keyedOneThreadTaskExecutor.destroy();
		executors.shutdown();
		super.doDispose();
	}
	
	public void setcurrentMetaManager(CurrentMetaManager currentMetaManager) {
		this.currentMetaManager = currentMetaManager;
	}
	
	public void setDcMetaCache(DcMetaCache dcMetaCache) {
		this.dcMetaCache = dcMetaCache;
	}
	
	public void setClientPool(SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool) {
		this.clientPool = clientPool;
	}
	
	public void setScheduled(ScheduledExecutorService scheduled) {
		this.scheduled = scheduled;
	}

	public void setExecutors(ExecutorService executors) {
		this.executors = executors;
	}
}
