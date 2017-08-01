package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.retry.RetryTemplate;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.OneThreadTaskExecutor;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.console.ConsoleService;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.retry.RetryDelay;
import com.ctrip.xpipe.retry.RetryNTimes;

import javax.annotation.Resource;
import java.util.concurrent.Executor;

/**
 * @author wenchao.meng
 *
 * Sep 7, 2016
 */
public class ConsoleNotifycationTask extends AbstractLifecycle implements MetaServerStateChangeHandler, TopElement{
	
	private int retryDelayBase = 10000;
	
	@Autowired
	private ConsoleService consoleService;

	@Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
	private Executor executors;
	
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
		oneThreadTaskExecutor = new OneThreadTaskExecutor(getRetryTemplate(), executors);
	}
	
	@Override
	protected void doDispose() throws Exception {
		oneThreadTaskExecutor.destroy();
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
	protected RetryTemplate getRetryTemplate() {
		return RetryNTimes.retryForEver(new RetryDelay(retryDelayBase));
	}
	
	public void setConsoleService(ConsoleService consoleService) {
		this.consoleService = consoleService;
	}

	@Override
	public void keeperMasterChanged(String clusterId, String shardId, Pair<String, Integer> newMaster) {
		//nothing to do
	}
}
