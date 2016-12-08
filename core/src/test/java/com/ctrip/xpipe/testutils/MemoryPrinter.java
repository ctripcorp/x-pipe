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
				printMemory();

			}

		}, 0, printIntervalMilli, TimeUnit.MILLISECONDS);

	}

	public void printMemory() {
		logger.info("max:{}, total:{}, free:{}", getMb(getMaxMemory()), getMb(getTotalMemory()), getMb(getFreeMemory()));
	}

	protected long getFreeMemory() {
		return Runtime.getRuntime().freeMemory();
	}

	protected long getTotalMemory() {
		return Runtime.getRuntime().totalMemory();
	}

	protected long getMaxMemory() {
		return Runtime.getRuntime().maxMemory();
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
