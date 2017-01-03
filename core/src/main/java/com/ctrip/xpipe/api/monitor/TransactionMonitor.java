package com.ctrip.xpipe.api.monitor;

import com.ctrip.xpipe.monitor.CatTransactionMonitor;

/**
 * @author wenchao.meng
 *
 * Jan 3, 2017
 */
public interface TransactionMonitor {
	
	public static TransactionMonitor  DEFAULT = new CatTransactionMonitor();
	
	void logTransaction(String type, String  name, Task task) throws Throwable;
	
	void logTransactionSwallowException(String type, String  name, Task task);
	
}
