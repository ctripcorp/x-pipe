package com.ctrip.xpipe.api.monitor;

import com.ctrip.xpipe.monitor.CatTransactionMonitor;

import java.util.concurrent.Callable;

/**
 * @author wenchao.meng
 *
 * Jan 3, 2017
 */
public interface TransactionMonitor {
	
	TransactionMonitor  DEFAULT = new CatTransactionMonitor();
	
	void logTransaction(String type, String  name, Task task) throws Exception;
	
	void logTransactionSwallowException(String type, String  name, Task task);

	<V> V logTransaction(String type, String  name, Callable<V> task) throws Exception;

	<V> V logTransactionSwallowException(String type, String  name, Callable<V> task);
}
