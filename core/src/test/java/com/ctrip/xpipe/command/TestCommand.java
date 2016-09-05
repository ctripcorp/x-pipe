package com.ctrip.xpipe.command;

import java.util.concurrent.ExecutionException;
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
		return "TestCommand";
	}

	@Override
	protected void doExecute() throws Exception {

		scheduled.schedule(new Runnable() {
			
			@Override
			public void run() {
				try {
					logger.info("[doExecute][begin]{}", this);
					if(e != null){
						future().setFailure(e);
					}else{
						future().setSuccess(successMessage);
					}
				}finally{
					logger.info("[doExecute][ end ]{}", this);
				}
			}
		}, sleepIntervalMilli, TimeUnit.MILLISECONDS);
	}

	@Override
	protected void doReset() throws InterruptedException, ExecutionException {
		
	}
}
