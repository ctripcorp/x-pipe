package com.ctrip.xpipe.redis.meta.server.job.manager;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.utils.MapUtils;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Apr 28, 2020
 */
public class KeyedJobManager<K> {

    private static Logger logger = LoggerFactory.getLogger(KeyedOneThreadTaskExecutor.class);

    private Map<K, JobManager> keyedJobManager = new ConcurrentHashMap<>();

    public void offer(K key, Command<?> job) {
        JobManager jobManager = getOrCreate(key);
        jobManager.offer(job);
    }

    private JobManager getOrCreate(K key) {
        return MapUtils.getOrCreate(keyedJobManager, key, new ObjectFactory<JobManager>() {

            @Override
            public JobManager create() {
                return createJobManager();
            }
        });
    }

    protected JobManager createJobManager() {
        return new MetaServerJobManager();
    }
}
