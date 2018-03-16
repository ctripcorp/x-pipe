package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.lifecycle.Destroyable;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 16, 2018
 */
public interface RetryCommandFactory<V> extends Destroyable {

    Command<V> createRetryCommand(Command<V> command);
}
