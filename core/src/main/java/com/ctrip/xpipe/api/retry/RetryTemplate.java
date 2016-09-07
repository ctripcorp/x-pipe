package com.ctrip.xpipe.api.retry;

import com.ctrip.xpipe.api.command.Command;

/**
 * @author wenchao.meng
 *
 * Jul 9, 2016
 */
public interface RetryTemplate<V> {
	
	V execute(Command<V> command) throws InterruptedException;

}
