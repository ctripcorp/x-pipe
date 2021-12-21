package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.command.RequestResponseCommand;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.command.LogIgnoreCommand;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.ExceptionUtils;
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
import java.util.concurrent.*;

public class BackupDcClusterShardAdjustJob extends AbstractCommand<Void> implements RequestResponseCommand<Void>, LogIgnoreCommand {

    private Long clusterDbId;

    private Long shardDbId;

    private Executor executors;

    private DcMetaCache dcMetaCache;

    private ScheduledExecutorService scheduled;

    private CurrentMetaManager currentMetaManager;

    private XpipeNettyClientKeyedObjectPool pool;

    private CommandFuture<Void> slaveOfJobFuture = null;

    public BackupDcClusterShardAdjustJob(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache,
                                         CurrentMetaManager currentMetaManager, Executor executor,
                                         ScheduledExecutorService schedule, XpipeNettyClientKeyedObjectPool pool) {
        this.clusterDbId = clusterDbId;
        this.shardDbId = shardDbId;
        this.executors = executor;
        this.dcMetaCache = dcMetaCache;
        this.scheduled = schedule;
        this.currentMetaManager = currentMetaManager;
        this.pool = pool;
    }

    @Override
    protected void doExecute() throws Exception {
        getLogger().debug("[doExecute]cluster_{}, shard_{}", clusterDbId, shardDbId);

        if (dcMetaCache.isCurrentDcPrimary(clusterDbId, shardDbId)) {
            getLogger().info("[doExecute][adjust skip] cluster_{}, shard_{} become primary dc", clusterDbId, shardDbId);
            future().setSuccess();
            return;
        }

        KeeperMeta keeperActive = currentMetaManager.getKeeperActive(clusterDbId, shardDbId);
        if(keeperActive == null){
            getLogger().info("[doExecute][keeper active null]cluster_{}, shard_{}", clusterDbId, shardDbId);
            future().setSuccess();
            return;
        }

        List<RedisMeta> redisNeedChange = getRedisNeedToChange(keeperActive);

        if (redisNeedChange.isEmpty()) {
            future().setSuccess();
            return;
        }

        getLogger().info("[doExecute][change state]cluster_{}, {}, {}", clusterDbId, keeperActive, redisNeedChange);

        slaveOfJobFuture = new DefaultSlaveOfJob(redisNeedChange, keeperActive.getIp(), keeperActive.getPort(), pool, scheduled, executors).execute();
        slaveOfJobFuture.addListener(new CommandFutureListener<Void>() {

            @Override
            public void operationComplete(CommandFuture<Void> commandFuture) throws Exception {
                if (!commandFuture.isSuccess()) {
                    getLogger().error("[operationComplete][fail]" + commandFuture.command(), commandFuture.cause());
                    future().setFailure(commandFuture.cause());
                } else {
                    future().setSuccess();
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

        for (RedisMeta redisMeta : dcMetaCache.getShardRedises(clusterDbId, shardDbId)) {
            if (future().isDone()) {
                return Collections.emptyList();
            }
            try {
                boolean change = false;
                RoleCommand roleCommand = new RoleCommand(
                        pool.getKeyPool(new DefaultEndPoint(redisMeta.getIp(), redisMeta.getPort())),
                        200,
                        false, scheduled);
                Role role = roleCommand.execute().get(200, TimeUnit.MILLISECONDS);

                if (role.getServerRole() == Server.SERVER_ROLE.MASTER) {
                    change = true;
                    getLogger().info("[getRedisNeedToChange][redis master, change to slave of keeper]{}, {}", redisMeta, keeperActive);
                } else if (role.getServerRole() == Server.SERVER_ROLE.SLAVE) {
                    SlaveRole slaveRole = (SlaveRole) role;
                    if (!keeperActive.getIp().equals(slaveRole.getMasterHost()) || !keeperActive.getPort().equals(slaveRole.getMasterPort())) {
                        getLogger().info("[getRedisNeedToChange][redis master not active keeper, change to slaveof keeper]{}, {}, {}", slaveRole, redisMeta, keeperActive);
                        change = true;
                    }
                } else {
                    getLogger().warn("[getRedisNeedToChange][role error]{}, {}", redisMeta, role);
                    continue;
                }
                if (change) {
                    redisesNeedChange.add(redisMeta);
                }
            } catch (TimeoutException timeoutException) {
                // do nothing
            } catch (Exception e) {
                if (!(ExceptionUtils.getRootCause(e) instanceof CommandTimeoutException)) {
                    getLogger().error("[getRedisNeedToChange]" + redisMeta, e);
                }
            }
        }
        return redisesNeedChange;
    }

    @Override
    public int getCommandTimeoutMilli() {
        return 1000;
    }
}
