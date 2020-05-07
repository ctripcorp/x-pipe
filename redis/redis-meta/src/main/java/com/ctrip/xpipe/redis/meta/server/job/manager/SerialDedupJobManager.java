package com.ctrip.xpipe.redis.meta.server.job.manager;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.RequestResponseCommand;
import com.ctrip.xpipe.concurrent.MutexableOneThreadTaskExecutor;
import com.ctrip.xpipe.concurrent.TaskExecutor;
import com.ctrip.xpipe.redis.meta.server.job.ChangePrimaryDcJob;

/**
 * @author chen.zhu
 * <p>
 * Apr 28, 2020
 */
public class SerialDedupJobManager extends AbstractDedupJobManager {
    public SerialDedupJobManager(TaskExecutor executors) {
        super(executors);
    }

//    @Override
//    public void offer(Command<?> task) {
//        if(task instanceof ChangePrimaryDcJob) {
//            jobs.clear();
//            ((MutexableOneThreadTaskExecutor) executors).clearAndExecuteCommand((ChangePrimaryDcJob) task);
//        } else {
//            super.offer(task);
//        }
//    }
}
