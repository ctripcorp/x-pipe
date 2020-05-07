package com.ctrip.xpipe.redis.meta.server.job.manager;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.meta.server.job.MetaServerJob;

/**
 * @author chen.zhu
 * <p>
 * Apr 28, 2020
 */
public class MetaServerJobManager implements JobManager {

    private SerialDedupJobManager serialJobManager;

    private ParallelDedupJobManager parallelJobManager;

    @Override
    public void offer(Command<?> task) {
        MetaServerJob<?> job = (MetaServerJob<?>) task;
        if(job.isSerial()) {
            serialJobManager.offer(job);
        } else {
            parallelJobManager.offer(job);
        }
    }
}
