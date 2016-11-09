package com.ctrip.xpipe.testutils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;

/**
 * @author wenchao.meng
 *
 *         Nov 9, 2016
 */
public class MemoryPrinter extends AbstractStartStoppable {

	private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(2);
	private int printIntervalMilli = 1000;
	
	public MemoryPrinter() {
	}

	public MemoryPrinter(int printIntervalMilli) {
		this.printIntervalMilli = printIntervalMilli;
	}

	@Override
	protected void doStart() throws Exception {
		
		scheduled.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				logger.info("max:{}, total:{}, free:{}", getMb(Runtime.getRuntime().maxMemory()), getMb(Runtime.getRuntime().totalMemory()), getMb(Runtime.getRuntime().freeMemory()));

			}
		}, 0, printIntervalMilli, TimeUnit.MILLISECONDS);

	}

	@Override
	protected void doStop() {
		scheduled.shutdownNow();
	}

	private String getMb(long maxMemory) {

		return String.format("%dMb", maxMemory / (1 << 20));
	}
	
	public static void main(String []argc) throws Exception{
		
		MemoryPrinter memoryPrinter = new MemoryPrinter();
		
		memoryPrinter.start();
		
		TimeUnit.SECONDS.sleep(5);
		
		memoryPrinter.stop();
		
		TimeUnit.SECONDS.sleep(5);
	}

}
