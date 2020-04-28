package com.ctrip.xpipe.redis.meta.server.job.manager;

import com.ctrip.xpipe.api.command.Command;

/**
 * @author chen.zhu
 * <p>
 * Apr 28, 2020
 */
public interface JobManager<T extends Command<?>> {

    void offer(Command<?> task);
}
