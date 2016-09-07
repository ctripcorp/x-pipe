package com.ctrip.xpipe.redis.meta.server.job;

import org.springframework.beans.factory.annotation.Autowired;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.retry.RetryTemplate;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.AbstractOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.core.console.ConsoleService;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.retry.RetryDelay;
import com.ctrip.xpipe.retry.RetryNTimes;

/**
 * @author wenchao.meng
 *
 * Sep 7, 2016
 */
public class ConsoleNotifycationTask extends AbstractOneThreadTaskExecutor implements MetaServerStateChangeHandler{
	
	private int retryDelayBase = 10000;
	
	@Autowired
	private ConsoleService consoleService;
	
	@Override
	public void keeperActiveElected(final String clusterId, final String shardId, final KeeperMeta activeKeeper) throws Exception {
		
		Command<Void> command = new AbstractCommand<Void>() {

			@Override
			public String getName() {
				return "[keeperActiveElected notify]";
			}

			@Override
			protected void doExecute() throws Exception {
				consoleService.keeperActiveChanged(FoundationService.DEFAULT.getDataCenter(), clusterId, shardId, activeKeeper);
				future().setSuccess();
			}

			@Override
			protected void doReset() {
				
			}
		};
		putCommand(command);
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected RetryTemplate getRetryTemplate() {
		return RetryNTimes.retryForEver(new RetryDelay(retryDelayBase));
	}
	
	public void setConsoleService(ConsoleService consoleService) {
		this.consoleService = consoleService;
	}
}
