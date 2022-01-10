package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * choose first configed alive slave in console
 *
 * @author wenchao.meng
 *         <p>
 *         Dec 9, 2016
 */
public class FirstNewMasterChooser extends AbstractNewMasterChooser {


    public FirstNewMasterChooser(XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled, Executor executors) {
        super(keyedObjectPool, scheduled, executors);
    }

    @Override
    protected RedisMeta doChooseFromAliveServers(List<RedisMeta> aliveServers) {

        if (aliveServers.size() >= 1) {
            return aliveServers.get(0);
        }
        return null;
    }

}
