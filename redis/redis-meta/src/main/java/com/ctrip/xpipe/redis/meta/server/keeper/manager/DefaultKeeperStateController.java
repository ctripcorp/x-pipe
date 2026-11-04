package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.KeeperDiskTypeUtils;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerServiceFactory;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperRemoveMetric;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperStateController;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig.CLIENT_POOL;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
public class DefaultKeeperStateController extends AbstractLifecycle implements KeeperStateController, TopElement{

	private Logger logger = LoggerFactory.getLogger(getClass());

	private KeeperRemoveMetric removeMetric = new KeeperRemoveMetric(MetricProxy.DEFAULT);

	private int addKeeperSuccessTimeoutMilli =    10000;
	private int removeKeeperSuccessTimeoutMilli = 10000;
		
	@Autowired
	private KeeperContainerServiceFactory keeperContainerServiceFactory;
	
	@Autowired
	private DcMetaCache dcMetaCache;

	private ExecutorService executors;

	@Resource( name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;

	@Resource(name = CLIENT_POOL)
	private XpipeNettyClientKeyedObjectPool clientKeyedObjectPool;
	
	private KeyedOneThreadTaskExecutor<Pair<Long, Long>> shardExecutor;
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		executors = DefaultExecutorFactory.createAllowCoreTimeout("keeperStateController", OsUtils.defaultMaxCoreThreadCount()).createExecutorService();
		shardExecutor = new KeyedOneThreadTaskExecutor<>(executors);
	}
	
	@Override
	public void addKeeper(KeeperTransMeta keeperTransMeta) {
		
		logger.info("[addKeeper]{}", keeperTransMeta);
		
		KeeperContainerService keeperContainerService = getKeeperContainerService(keeperTransMeta);
		shardExecutor.execute(new Pair<>(keeperTransMeta.getClusterDbId(), keeperTransMeta.getShardDbId()),
				createAddKeeperCommand(keeperContainerService, keeperTransMeta, scheduled, addKeeperSuccessTimeoutMilli));
	}

	protected Command<?> createAddKeeperCommand(KeeperContainerService keeperContainerService,
			KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled, int addKeeperSuccessTimeoutMilli) {
		// TFS: never destroy a stale keeper found on the target port, its shared store may be live elsewhere
		return new AddKeeperCommand(keeperContainerService, keeperTransMeta, clientKeyedObjectPool, scheduled,
				addKeeperSuccessTimeoutMilli, !isBm(keeperTransMeta));
	}

	@Override
	public void removeKeeper(KeeperTransMeta keeperTransMeta, boolean purge) {

		logger.info("[removeKeeper]{}, purge={}", keeperTransMeta, purge);

		KeeperContainerService keeperContainerService = getKeeperContainerService(keeperTransMeta);
		boolean bm = isBm(keeperTransMeta);
		Command<?> command = createRemoveKeeperCommand(keeperContainerService, keeperTransMeta, purge, bm,
				scheduled, removeKeeperSuccessTimeoutMilli);
		trackRemoveKeeper(command, keeperTransMeta, bm, isRelease(purge, bm));
		shardExecutor.execute(new Pair<>(keeperTransMeta.getClusterDbId(), keeperTransMeta.getShardDbId()), command);
	}

	/**
	 * purge (cluster/shard deleted, or shard left with no keepers) always deletes the store; a BM
	 * keeper always deletes its private store; otherwise (TFS, non-purge: migration, dead-keeper
	 * cleanup) release only — the shared store must outlive this keeper instance.
	 */
	private boolean isRelease(boolean purge, boolean bm) {
		return !purge && !bm;
	}

	protected Command<?> createRemoveKeeperCommand(KeeperContainerService keeperContainerService, KeeperTransMeta keeperTransMeta,
			boolean purge, boolean bm, ScheduledExecutorService scheduled, int removeKeeperSuccessTimeoutMilli) {
		if (isRelease(purge, bm)) {
			return createReleaseKeeperCommand(keeperContainerService, keeperTransMeta, scheduled, removeKeeperSuccessTimeoutMilli);
		}
		return createDeleteKeeperCommand(keeperContainerService, keeperTransMeta, scheduled, removeKeeperSuccessTimeoutMilli);
	}

	/** tfsId is resolved now: by completion the keeper may already be gone from meta. */
	private void trackRemoveKeeper(Command<?> command, KeeperTransMeta keeperTransMeta, boolean bm, boolean release) {
		try {
			KeeperMeta keeperMeta = keeperTransMeta.getKeeperMeta();
			String tfsId = bm ? null : getTfsFsId(keeperMeta);
			command.future().addListener(commandFuture ->
					removeMetric.trackRemoveKeeper(keeperMeta, bm, release, tfsId, commandFuture.isSuccess()));
		} catch (Throwable th) {
			logger.debug("[trackRemoveKeeper] fail", th);
		}
	}

	protected String getTfsFsId(KeeperMeta keeperMeta) {
		KeeperContainerMeta keeperContainer = dcMetaCache.getKeeperContainer(keeperMeta);
		return keeperContainer == null ? null : keeperContainer.getTfsFsId();
	}

	protected boolean isBm(KeeperTransMeta keeperTransMeta) {
		return KeeperDiskTypeUtils.isBm(dcMetaCache.getKeeperContainer(keeperTransMeta.getKeeperMeta()));
	}

	protected Command<?> createDeleteKeeperCommand(KeeperContainerService keeperContainerService,
			KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled,
			int removeKeeperSuccessTimeoutMilli) {
		return new DeleteKeeperCommand(keeperContainerService, keeperTransMeta, scheduled, removeKeeperSuccessTimeoutMilli);
	}

	protected Command<?> createReleaseKeeperCommand(KeeperContainerService keeperContainerService,
			KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled,
			int removeKeeperSuccessTimeoutMilli) {
		return new ReleaseKeeperCommand(keeperContainerService, keeperTransMeta, scheduled, removeKeeperSuccessTimeoutMilli);
	}

	protected KeeperContainerService getKeeperContainerService(KeeperTransMeta keeperTransMeta) {
		
		KeeperContainerMeta keeperContainerMeta = dcMetaCache.getKeeperContainer(keeperTransMeta.getKeeperMeta());
		KeeperContainerService keeperContainerService = keeperContainerServiceFactory.getOrCreateKeeperContainerService(keeperContainerMeta);
		return keeperContainerService;
	}

	public void setExecutors(ExecutorService executors) {
		this.executors = executors;
	}

	@VisibleForTesting
	protected void setMetricProxy(MetricProxy metricProxy) {
		this.removeMetric = new KeeperRemoveMetric(metricProxy);
	}

	@Override
	protected void doDispose() throws Exception {

		shardExecutor.destroy();
		executors.shutdown();
		super.doDispose();
	}
}
