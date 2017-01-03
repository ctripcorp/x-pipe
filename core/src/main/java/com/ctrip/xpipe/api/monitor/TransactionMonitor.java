package com.ctrip.xpipe.api.monitor;

/**
 * @author wenchao.meng
 *
 * Jan 3, 2017
 */
public interface TransactionMonitor {
	
	public static TransactionMonitor  DEFAULT = null;
	
	void logTransaction(String type, String  name);
	
}
