package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.command.RetryCommandFactory;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.concurrent.OneThreadTaskExecutor;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.console.ConsoleService;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Sep 7, 2016
 */
public class ConsoleNotifycationTask extends AbstractLifecycle implements MetaServerStateChangeHandler, TopElement{
	
	private int retryDelayBase = 10000;
	private int maxScheduled = 4;
	
	@Autowired
	private ConsoleService consoleService;

	@Autowired
	private DcMetaCache dcMetaCache;

	@Autowired
	private MetaServerConfig config;

	private ExecutorService executors;

	private ScheduledExecutorService scheduled;

	private String dc = FoundationService.DEFAULT.getDataCenter();
	
	private OneThreadTaskExecutor oneThreadTaskExecutor;

	public ConsoleNotifycationTask(){
		
	}
	
	public ConsoleNotifycationTask(int retryDelayBase) {
		this.retryDelayBase = retryDelayBase;
	}
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		scheduled = Executors.newScheduledThreadPool(OsUtils.getMultiCpuOrMax(1, maxScheduled), XpipeThreadFactory.create("ConsoleNotifycationTaskScheduled"));
		executors = DefaultExecutorFactory.createAllowCoreTimeout("ConsoleNotifycationTask", OsUtils.defaultMaxCoreThreadCount()).createExecutorService();
		oneThreadTaskExecutor = new OneThreadTaskExecutor(getRetryFactory(), executors, config.getConsoleNotifycationTaskQueueSize());
	}
	
	@Override
	protected void doDispose() throws Exception {
		oneThreadTaskExecutor.destroy();
		executors.shutdown();
		scheduled.shutdown();
		super.doDispose();
	}
	
	@Override
	public void keeperActiveElected(final Long clusterDbId, final Long shardDbId, final KeeperMeta activeKeeper) {
		logger.info("[keeperActiveElected][called]cluster_{},shard_{},{}",clusterDbId,shardDbId,activeKeeper);
		Command<Void> command = new AbstractCommand<Void>() {

			@Override
			public String getName() {
				return "[keeperActiveElected notify]";
			}

			@Override
			protected void doExecute() throws Exception {
				Pair<String, String> clusterShard = dcMetaCache.clusterShardDbId2Name(clusterDbId, shardDbId);
				logger.info("[keeperActiveElected][execute]cluster_{}:{},shard_{}:{},{}",clusterDbId, clusterShard.getKey(),
						shardDbId, clusterShard.getValue(), activeKeeper);
				consoleService.keeperActiveChanged(dc, clusterShard.getKey(), clusterShard.getValue(), activeKeeper);
				future().setSuccess();
			}

			@Override
			protected void doReset() {
				
			}
		};
		oneThreadTaskExecutor.executeCommand(command);
	}

	@Override
	public void applierActiveElected(final Long clusterDbId, final Long shardDbId, final ApplierMeta activeApplier, String srcSids) {
		logger.info("[applierActiveElected][called]cluster_{},shard_{},{}", clusterDbId, shardDbId, activeApplier);
		Command<Void> command = new AbstractCommand<Void>() {

			@Override
			public String getName() {
			    return "[applierActiveElected notify]";
			}

			@Override
			protected void doExecute() throws Throwable {
				Pair<String, String> clusterShard = dcMetaCache.clusterShardDbId2Name(clusterDbId, shardDbId);
				logger.info("[applierActiveElected][execute]cluster_{}:{},shard_{}:{},{}", clusterDbId, clusterShard.getKey(),
						shardDbId, clusterShard.getValue(), activeApplier);
				consoleService.applierActiveChanged(dc, clusterShard.getKey(), clusterShard.getValue(), activeApplier);
				future().setSuccess();
			}

			@Override
			protected void doReset() {}
		};
		oneThreadTaskExecutor.executeCommand(command);
	}

	
	@SuppressWarnings("rawtypes")
	protected RetryCommandFactory getRetryFactory() {
		return DefaultRetryCommandFactory.retryForever(scheduled, retryDelayBase);
	}
	
	public void setConsoleService(ConsoleService consoleService) {
		this.consoleService = consoleService;
	}

	@VisibleForTesting
	protected void setDcMetaCache(DcMetaCache dcMetaCache) {
		this.dcMetaCache = dcMetaCache;
	}

}
