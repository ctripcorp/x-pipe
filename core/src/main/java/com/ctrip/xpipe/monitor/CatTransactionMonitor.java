package com.ctrip.xpipe.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

import java.util.concurrent.Callable;

/**
 * @author wenchao.meng
 *
 * Jan 3, 2017
 */
public class CatTransactionMonitor implements TransactionMonitor{
	
	public static Logger logger = LoggerFactory.getLogger(CatTransactionMonitor.class);

	@Override
	public void logTransactionSwallowException(String type, String name, Task task) {
		
		Transaction transaction = Cat.newTransaction(type, name);
		try{
			task.go();
			transaction.setStatus(Transaction.SUCCESS);
		}catch(Throwable th){
			transaction.setStatus(th);
			logger.error("[logTransaction]" + type + "," + name + "," + task, th);
		}finally{
			transaction.complete();
		}
	}

	@Override
	public void logTransaction(String type, String name, Task task) throws Exception {

		Transaction transaction = Cat.newTransaction(type, name);
		try{
			task.go();
			transaction.setStatus(Transaction.SUCCESS);
		}catch(Exception th){
			transaction.setStatus(th);
			throw th;
		}finally{
			transaction.complete();
		}
	}

	@Override
	public <V> V logTransaction(String type, String name, Callable<V> task) throws Exception {
		Transaction transaction = Cat.newTransaction(type, name);
		try{
			V result = task.call();
			transaction.setStatus(Transaction.SUCCESS);
			return result;
		}catch(Exception th){
			transaction.setStatus(th);
			throw th;
		}finally{
			transaction.complete();
		}
	}

	@Override
	public <V> V logTransactionSwallowException(String type, String name, Callable<V> task) {

		Transaction transaction = Cat.newTransaction(type, name);
		try{
			V result = task.call();
			transaction.setStatus(Transaction.SUCCESS);
			return result;
		}catch(Throwable th){
			transaction.setStatus(th);
			logger.error("[logTransaction]" + type + "," + name + "," + task, th);
		}finally{
			transaction.complete();
		}
		return null;
	}
}
