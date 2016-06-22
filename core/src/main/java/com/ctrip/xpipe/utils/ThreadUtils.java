package com.ctrip.xpipe.utils;

/**
 * @author wenchao.meng
 *
 * Jun 22, 2016
 */
public class ThreadUtils {
	
	public static Thread newThread(String threadName, Runnable runnable){
		
		Thread thread = new Thread(runnable);
		thread.setName(threadName);
		return thread;
	}

}
