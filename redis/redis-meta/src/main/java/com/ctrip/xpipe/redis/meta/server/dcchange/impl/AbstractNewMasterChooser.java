package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.meta.server.dcchange.NewMasterChooser;
import com.ctrip.xpipe.redis.meta.server.dcchange.exception.ChooseNewMasterFailException;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

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

    protected Executor executors;

    public AbstractNewMasterChooser(XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled, Executor executors) {
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

        logger.debug("[choose]{}, {}", masters, aliveServers);
        if (masters.size() == 0) {
            if(aliveServers.size() == 0){
                throw ChooseNewMasterFailException.noAliveServer(redises);
            }
            newMaster = doChooseFromAliveServers(aliveServers);
        } else if (masters.size() == 1) {
            logger.info("[choose][already has master]{}", masters);
            newMaster = masters.get(0);
        } else {
            throw ChooseNewMasterFailException.multiMaster(masters, redises);
        }
        return newMaster;
    }

    protected Pair<List<RedisMeta>, List<RedisMeta>> getMasters(List<RedisMeta> allRedises) {

        List<RedisMeta> masters = new LinkedList<>();
        List<RedisMeta> tmpAliveServers = new LinkedList<>();

        ParallelCommandChain commandChain = new ParallelCommandChain(executors);
        for (RedisMeta redisMeta : allRedises) {
            SimpleObjectPool<NettyClient> clientPool = keyedObjectPool.getKeyPool(new DefaultEndPoint(redisMeta.getIp(), redisMeta.getPort()));
            RoleCommand cmd = new RoleCommand(clientPool, CHECK_NEW_MASTER_TIMEOUT_SECONDS*1000, true, scheduled);
            cmd.future().addListener(commandFuture -> {
                if (commandFuture.isSuccess()) {
                    SERVER_ROLE role = commandFuture.get().getServerRole();
                    if (role == SERVER_ROLE.MASTER) {
                        synchronized (masters) {
                            masters.add(redisMeta);
                        }
                    } else if (role == SERVER_ROLE.SLAVE) {
                        synchronized (tmpAliveServers) {
                            tmpAliveServers.add(redisMeta);
                        }
                    }
                } else {
                    logger.error("[getMasters] isMaster {}", redisMeta, commandFuture.cause());
                }
            });

            commandChain.add(cmd);
        }

        try {
            commandChain.execute().get(CHECK_NEW_MASTER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Throwable th) {
            logger.error("[getMasters] execute fail {}", allRedises, th);
        }

        List<RedisMeta> aliveServers = sortAccording(allRedises, tmpAliveServers);
        return new Pair<>(masters, aliveServers);
    }

    public List<RedisMeta> sortAccording(List<RedisMeta> according, List<RedisMeta> tmpResult){

        List<RedisMeta> result = new LinkedList<>();
        for(RedisMeta seq : according){

            boolean exists = false;
            for(RedisMeta real : tmpResult){
                if(seq.equalsWithIpPort(real)){
                    exists = true;
                    break;
                }
            }
            if(exists){
                result.add(seq);
            }
        }
        return result;
    }

    protected abstract RedisMeta doChooseFromAliveServers(List<RedisMeta> aliveServers);

}
