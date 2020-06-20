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
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.utils.OsUtils;
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
		oneThreadTaskExecutor = new OneThreadTaskExecutor(getRetryFactory(), executors);
	}
	
	@Override
	protected void doDispose() throws Exception {
		oneThreadTaskExecutor.destroy();
		executors.shutdown();
		scheduled.shutdown();
		super.doDispose();
	}
	
	@Override
	public void keeperActiveElected(final String clusterId, final String shardId, final KeeperMeta activeKeeper) {
		logger.info("[keeperActiveElected][called]{},{},{}",clusterId,shardId,activeKeeper);
		Command<Void> command = new AbstractCommand<Void>() {

			@Override
			public String getName() {
				return "[keeperActiveElected notify]";
			}

			@Override
			protected void doExecute() throws Exception {
				logger.info("[keeperActiveElected][execute]{},{},{}",clusterId,shardId,activeKeeper);
				consoleService.keeperActiveChanged(dc, clusterId, shardId, activeKeeper);
				future().setSuccess();
			}

			@Override
			protected void doReset() {
				
			}
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

}
