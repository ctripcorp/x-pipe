package com.ctrip.xpipe.redis.meta.server.job.manager;

import com.ctrip.xpipe.concurrent.TaskExecutor;

/**
 * @author chen.zhu
 * <p>
 * Apr 28, 2020
 */
public class ParallelDedupJobManager extends AbstractDedupJobManager {
    public ParallelDedupJobManager(TaskExecutor executors) {
        super(executors);
    }
}
