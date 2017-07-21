package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.PingCommand;

/**
 * choose first configed alive slave in console
 *
 * @author wenchao.meng
 *         <p>
 *         Dec 9, 2016
 */
public class FirstNewMasterChooser extends AbstractNewMasterChooser {


    public FirstNewMasterChooser(XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled, ExecutorService executors) {
        super(keyedObjectPool, scheduled, executors);
    }

    @Override
    protected RedisMeta doChooseFromAliveServers(List<RedisMeta> aliveServers) {

        if (aliveServers.size() >= 1) {
            return aliveServers.get(0);
        }
        return null;
    }

    private boolean isAlive(RedisMeta redisMeta) {

        SimpleObjectPool<NettyClient> clientPool = keyedObjectPool.getKeyPool(new InetSocketAddress(redisMeta.getIp(), redisMeta.getPort()));
        try {
            new PingCommand(clientPool, scheduled).execute().get();
            return true;
        } catch (InterruptedException | ExecutionException e) {
            logger.info("[isAlive]" + redisMeta, e);
        }
        return false;
    }
}
