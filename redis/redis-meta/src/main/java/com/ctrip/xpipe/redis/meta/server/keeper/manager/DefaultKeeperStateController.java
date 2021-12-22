package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerServiceFactory;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperStateController;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.OsUtils;
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
	
	private int addKeeperSuccessTimeoutMilli =    180000;
	private int removeKeeperSuccessTimeoutMilli = 60000;
		
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
		return new AddKeeperCommand(keeperContainerService, keeperTransMeta, clientKeyedObjectPool, scheduled, addKeeperSuccessTimeoutMilli);
	}

	@Override
	public void removeKeeper(KeeperTransMeta keeperTransMeta) {
		
		logger.info("[removeKeeper]{}", keeperTransMeta);
		KeeperContainerService keeperContainerService = getKeeperContainerService(keeperTransMeta);
		shardExecutor.execute(new Pair<>(keeperTransMeta.getClusterDbId(), keeperTransMeta.getShardDbId()),
				createDeleteKeeperCommand(keeperContainerService, keeperTransMeta, scheduled, removeKeeperSuccessTimeoutMilli));
	}

	protected Command<?> createDeleteKeeperCommand(KeeperContainerService keeperContainerService,
			KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled,
			int removeKeeperSuccessTimeoutMilli) {
		return new DeleteKeeperCommand(keeperContainerService, keeperTransMeta, scheduled, removeKeeperSuccessTimeoutMilli);
	}

	protected KeeperContainerService getKeeperContainerService(KeeperTransMeta keeperTransMeta) {
		
		KeeperContainerMeta keeperContainerMeta = dcMetaCache.getKeeperContainer(keeperTransMeta.getKeeperMeta());
		KeeperContainerService keeperContainerService = keeperContainerServiceFactory.getOrCreateKeeperContainerService(keeperContainerMeta);
		return keeperContainerService;
	}

	public void setExecutors(ExecutorService executors) {
		this.executors = executors;
	}

	@Override
	protected void doDispose() throws Exception {

		shardExecutor.destroy();
		executors.shutdown();
		super.doDispose();
	}
}
