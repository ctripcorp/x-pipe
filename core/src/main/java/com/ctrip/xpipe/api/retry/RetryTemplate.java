package com.ctrip.xpipe.api.retry;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.lifecycle.Destroyable;

/**
 * @author wenchao.meng
 *
 * Jul 9, 2016
 */
public interface RetryTemplate<V> extends Destroyable{
	
	V execute(Command<V> command) throws Exception;

}
