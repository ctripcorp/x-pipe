package com.ctrip.xpipe.monitor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

/**
 * @author wenchao.meng
 *
 *         Aug 29, 2016
 */
public class CatUtils {

	private static Logger logger = LoggerFactory.getLogger(CatUtils.class);
	
	public static final String CAT_ENABLED_KEY = "cat.client.enabled";
	
	private static final boolean catEnabled = Boolean.parseBoolean(System.getProperty(CAT_ENABLED_KEY, "true"));
	
	private static ExecutorService executors = Executors.newCachedThreadPool(XpipeThreadFactory.create("CatUtils"));

	public static void newFutureTaskTransaction(final String type, final String name, final CommandFuture<?> future) {
		
		if(!catEnabled){
			return;
		}

		logger.debug("[newFutureTaskTransaction]{}, {}", type, name);
		executors.execute(new Runnable() {

			@Override
			public void run() {

				Transaction transaction = Cat.newTransaction(type, name);
				try {
					boolean result = future.await(1, TimeUnit.HOURS);
					logger.debug("[newFutureTaskTransaction][complete]{}, {}, {}, ({})", type, name, future.isSuccess(),
							result);
					if (result && future.isSuccess()) {
						transaction.setStatus(Transaction.SUCCESS);
					} else {
						if (!result) {
							transaction.setStatus(new TimeoutException("wait for transaction timeout...[1 hour]"));
						} else {
							transaction.setStatus(future.cause());
						}
					}
				} catch (Throwable th) {
					logger.error("[run]" + type + "," + name, th);
					transaction.setStatus(th);
				} finally {
					transaction.complete();
				}
			}
		});
	}
}
