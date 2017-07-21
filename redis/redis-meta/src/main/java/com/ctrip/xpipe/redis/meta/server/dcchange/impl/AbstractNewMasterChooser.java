package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.meta.server.dcchange.NewMasterChooser;

/**
 * @author wenchao.meng
 *         <p>
 *         Dec 9, 2016
 */
public abstract class AbstractNewMasterChooser implements NewMasterChooser {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    public static final int CHECK_NEW_MASTER_TIMEOUT_SECONDS = Integer.parseInt(System.getProperty("CHECK_NEW_MASTER_TIMEOUT_SECONDS", "2"));

    protected XpipeNettyClientKeyedObjectPool keyedObjectPool;

    protected RedisMeta newMaster = null;

    protected ScheduledExecutorService scheduled;

    protected ExecutorService executors;

    public AbstractNewMasterChooser(XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled, ExecutorService executors) {
        this.keyedObjectPool = keyedObjectPool;
        this.scheduled = scheduled;
        this.executors = executors;
    }

    public RedisMeta getLastChoosenMaster() {
        return newMaster;
    }

    @Override
    public RedisMeta choose(List<RedisMeta> redises) {

        Pair<List<RedisMeta>, List<RedisMeta>> pair = getMasters(redises);

        List<RedisMeta> masters = pair.getKey();
        List<RedisMeta> aliveServers = pair.getValue();

        if (masters.size() == 0) {
            newMaster = doChooseFromAliveServers(aliveServers);
        } else if (masters.size() == 1) {
            logger.info("[choose][already has master]{}", masters);
            newMaster = masters.get(0);
        } else {
            throw new IllegalStateException("multi master there, can not choose a new master " + masters);
        }
        return newMaster;
    }

    protected Pair<List<RedisMeta>, List<RedisMeta>> getMasters(List<RedisMeta> allRedises) {

        List<RedisMeta> masters = new LinkedList<>();
        List<RedisMeta> aliveServers = new LinkedList<>();

        CountDownLatch latch = new CountDownLatch(allRedises.size());

        for (RedisMeta redisMeta : allRedises) {

            executors.execute(new AbstractExceptionLogTask() {

                @Override
                protected void doRun() throws Exception {
                    try {
                        SERVER_ROLE role = serverRole(redisMeta);
                        if (role == SERVER_ROLE.MASTER) {
                            masters.add(redisMeta);
                        }
                        if (role != SERVER_ROLE.UNKNOWN) {
                            aliveServers.add(redisMeta);
                        }
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        try {
            latch.await(CHECK_NEW_MASTER_TIMEOUT_SECONDS * 2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("[getMasters]" + allRedises, e);
        }
        return new Pair<>(masters, aliveServers);
    }

    protected SERVER_ROLE serverRole(RedisMeta redisMeta) {

        try {
            SimpleObjectPool<NettyClient> clientPool = keyedObjectPool.getKeyPool(new InetSocketAddress(redisMeta.getIp(), redisMeta.getPort()));
            Role role = new RoleCommand(clientPool, true, scheduled).execute().get(CHECK_NEW_MASTER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return role.getServerRole();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error("[isMaster]" + redisMeta, e);
        }
        return SERVER_ROLE.UNKNOWN;
    }

    protected abstract RedisMeta doChooseFromAliveServers(List<RedisMeta> aliveServers);

}
