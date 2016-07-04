package com.ctrip.xpipe.command;




import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author wenchao.meng
 *
 * Jun 26, 2016
 */
public abstract class AbstractCommand<V> implements Command<V>{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected ScheduledExecutorService scheduled;
	
	protected CommandFuture<V> future = new DefaultCommandFuture<>();

	
	public AbstractCommand(ScheduledExecutorService scheduled) {
		this.scheduled = scheduled;
	}

	public AbstractCommand() {
		this.scheduled = Executors.newScheduledThreadPool(OsUtils.getCpuCount(), XpipeThreadFactory.create("command-" + getName()));
	}

	@Override
	public CommandFuture<V> execute() throws CommandExecutionException{
		return doExecute();
	}

	protected abstract CommandFuture<V> doExecute() throws CommandExecutionException;

	@Override
	public CommandFuture<V> execute(final int time, TimeUnit timeUnit) {
		
		final ScheduledFuture<?> scheduleFuture = scheduled.schedule(new Runnable() {
			
			@Override
			public void run() {
				try {
					execute();
				} catch (CommandExecutionException e) {
					logger.error("[run]" + AbstractCommand.this, e);
				}
			}
		}, time, timeUnit);

		future.addListener(new CommandFutureListener<V>() {

			@Override
			public void operationComplete(CommandFuture<V> commandFuture) throws Exception {
				if(commandFuture.isCancelled()){
					logger.info("[command canceled][cancel execution]{}", time);
					scheduleFuture.cancel(false);
				}
			}
		});
		return future;
	}
	
}
