package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 07, 2017
 */
public class InfoReplicationCommand extends InfoCommand{

    public InfoReplicationCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        super(clientPool, INFO_TYPE.REPLICATION, scheduled);
    }





}
