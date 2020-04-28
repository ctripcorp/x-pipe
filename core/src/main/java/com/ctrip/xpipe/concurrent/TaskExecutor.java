package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.api.command.Command;

/**
 * @author chen.zhu
 * <p>
 * Apr 28, 2020
 */
public interface TaskExecutor {
    void executeCommand(Command<?> command);
}
