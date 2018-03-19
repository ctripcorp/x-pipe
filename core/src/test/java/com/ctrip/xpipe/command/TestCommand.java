package com.ctrip.xpipe.command;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * Jul 15, 2016
 */
public class TestCommand extends AbstractCommand<String>{

	private Exception e;
	private String successMessage;
	private int sleepIntervalMilli = 100;
	private ScheduledExecutorService scheduled;
	private boolean beginExecute = false;
	
	
	private ScheduledFuture<?> future;
	
	public TestCommand(String successMessage) {
		this(null, successMessage, 100);
	}

	public TestCommand(Exception e) {
		this(e, "OK", 100);
	}
	
	public TestCommand(String successMessage, int sleepInterval) {
		this(null, successMessage, sleepInterval);
	}

	public TestCommand(Exception e, int sleepInterval) {
		this(e, "OK", sleepInterval);
	}

	public TestCommand(Exception e, String successMessage, int sleepInterval) {
		this.e = e;
		this.successMessage = successMessage;
		this.sleepIntervalMilli = sleepInterval;
	}

	@Override
	public String getName() {
		return "TestCommand@" + System.identityHashCode(this);
	}

	@Override
	protected void doExecute() throws Exception {
		beginExecute = true;

		scheduled = Executors.newScheduledThreadPool(1);
		future = scheduled.schedule(new AbstractExceptionLogTask() {
			
			@Override
			public void doRun() {
				try {
					logger.debug("[doExecute][begin]{}", this);
					if(e != null){
						future().setFailure(e);
					}else{
						future().setSuccess(successMessage);
					}
				}finally{
					logger.debug("[doExecute][ end ]{}", this);
					scheduled.shutdown();
				}
			}
		}, sleepIntervalMilli, TimeUnit.MILLISECONDS);
	}

	public boolean isBeginExecute() {
		return beginExecute;
	}
	
	@Override
	protected void doReset(){
		
	}
	
	public String getSuccessMessage() {
		return successMessage;
	}
	
	@Override
	protected void doCancel() {
		super.doCancel();
		
		if(future != null){
			future.cancel(true);
		}
	}
}
