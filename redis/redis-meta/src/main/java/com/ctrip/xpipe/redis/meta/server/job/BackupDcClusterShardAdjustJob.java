package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

public class BackupDcClusterShardAdjustJob extends AbstractCommand<Void> {

    private String cluster;

    private String shard;

    private Executor executors;

    private DcMetaCache dcMetaCache;

    private ScheduledExecutorService scheduled;

    private CurrentMetaManager currentMetaManager;

    private XpipeNettyClientKeyedObjectPool pool;

    private CommandFuture<Void> slaveOfJobFuture = null;

    public BackupDcClusterShardAdjustJob(String cluster, String shard, DcMetaCache dcMetaCache,
                                         CurrentMetaManager currentMetaManager, Executor executor,
                                         ScheduledExecutorService schedule, XpipeNettyClientKeyedObjectPool pool) {
        this.shard = shard;
        this.cluster = cluster;
        this.executors = executor;
        this.dcMetaCache = dcMetaCache;
        this.scheduled = schedule;
        this.currentMetaManager = currentMetaManager;
        this.pool = pool;
    }

    @Override
    protected void doExecute() throws Exception {
        logger.debug("[doExecute]{}, {}", cluster, shard);

        if (dcMetaCache.isCurrentDcPrimary(cluster, shard)) {
            logger.info("[doExecute][adjust skip] {}, {} become primary dc", cluster, shard);
            return;
        }

        KeeperMeta keeperActive = currentMetaManager.getKeeperActive(cluster, shard);
        if(keeperActive == null){
            logger.info("[doExecute][keeper active null]{}, {}", cluster, shard);
            return;
        }

        List<RedisMeta> redisNeedChange = getRedisNeedToChange(keeperActive);

        if (redisNeedChange.isEmpty()) {
            return;
        }

        logger.info("[doExecute][change state]{}, {}, {}", cluster, keeperActive, redisNeedChange);

        slaveOfJobFuture = new DefaultSlaveOfJob(redisNeedChange, keeperActive.getIp(), keeperActive.getPort(), pool, scheduled, executors).execute();
        slaveOfJobFuture.addListener(new CommandFutureListener<Void>() {

            @Override
            public void operationComplete(CommandFuture<Void> commandFuture) throws Exception {
                if (!commandFuture.isSuccess()) {
                    logger.error("[operationComplete][fail]" + commandFuture.command(), commandFuture.cause());
                }
            }
        });
    }

    @Override
    protected void doReset() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doCancel() {
        if (null != slaveOfJobFuture && !slaveOfJobFuture.isDone()) {
            slaveOfJobFuture.cancel(true);
        }
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    protected List<RedisMeta> getRedisNeedToChange(KeeperMeta keeperActive) {

        List<RedisMeta> redisesNeedChange = new LinkedList<>();

        for (RedisMeta redisMeta : dcMetaCache.getShardRedises(cluster, shard)) {
            if (future().isDone()) {
                return Collections.emptyList();
            }
            try {
                boolean change = false;
                RoleCommand roleCommand = new RoleCommand(
                        pool.getKeyPool(new DefaultEndPoint(redisMeta.getIp(), redisMeta.getPort())),
                        500,
                        false, scheduled);
                Role role = roleCommand.execute().get();

                if (role.getServerRole() == Server.SERVER_ROLE.MASTER) {
                    change = true;
                    logger.info("[getRedisNeedToChange][redis master, change to slave of keeper]{}, {}", redisMeta, keeperActive);
                } else if (role.getServerRole() == Server.SERVER_ROLE.SLAVE) {
                    SlaveRole slaveRole = (SlaveRole) role;
                    if (!keeperActive.getIp().equals(slaveRole.getMasterHost()) || !keeperActive.getPort().equals(slaveRole.getMasterPort())) {
                        logger.info("[getRedisNeedToChange][redis master not active keeper, change to slaveof keeper]{}, {}, {}", slaveRole, redisMeta, keeperActive);
                        change = true;
                    }
                } else {
                    logger.warn("[getRedisNeedToChange][role error]{}, {}", redisMeta, role);
                    continue;
                }
                if (change) {
                    redisesNeedChange.add(redisMeta);
                }
            } catch (Exception e) {
                logger.error("[getRedisNeedToChange]" + redisMeta, e);
            }
        }
        return redisesNeedChange;
    }

}
