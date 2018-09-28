package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.exception.ExceptionUtils;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * Dec 21, 2016
 */
public class ScheduleCommandWrapper<V> extends AbstractCommand<V>{
	
	private ScheduledExecutorService scheduled;
	private int time;
	private TimeUnit timeUnit;
	private Command<V> command;
	
	public ScheduleCommandWrapper(Command<V> command, ScheduledExecutorService scheduled, int time, TimeUnit timeUnit) {
		this.command = command;
		this.scheduled = scheduled;
		this.time = time;
		this.timeUnit = timeUnit;
	}

	@Override
	public String getName() {
		return "ScheduleCommandWrapper:" + command;
	}

	@Override
	protected void doExecute() throws Exception {

		final ScheduledFuture<?> scheduleFuture = scheduled.schedule(new AbstractExceptionLogTask() {
			
			@Override
			protected void doRun() throws Exception {
				try{
					command.execute().addListener(new CommandFutureListener<V>() {

						@Override
						public void operationComplete(CommandFuture<V> commandFuture) throws Exception {
							if(commandFuture.isSuccess()){
								future().setSuccess(commandFuture.get());
							}else{
								future().setFailure(ExceptionUtils.getRootCause(commandFuture.cause()));
							}
						}
					});
				}catch(Exception e){
					future().setFailure(ExceptionUtils.getRootCause(e));
				}
			}
		}, time, timeUnit);

		future().addListener(new CommandFutureListener<V>() {

			@Override
			public void operationComplete(CommandFuture<V> commandFuture) throws Exception {
				if(commandFuture.isCancelled()){
					logger.info("[command canceled][cancel execution]{}", time);
					command.future().cancel(true);
					scheduleFuture.cancel(false);
				}
			}
		});
	}

	@Override
	protected void doReset() {
		
	}
}
