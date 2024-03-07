package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.command.CommandChain;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.*;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterRole;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisMaster;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author lishanglin
 * date 2021/12/14
 * [MAC] update fd limit to 8192 before test
 * sudo sysctl -w kern.maxfiles=1048600
 * sudo sysctl -w kern.maxfilesperproc=1048600
 * ulimit -n 8124
 */
public class KeeperFastStateChangeTest extends AbstractKeeperIntegratedSingleDc {

    @Override
    protected KeeperConfig getKeeperConfig() {
        TestKeeperConfig keeperConfig = new TestKeeperConfig();
        keeperConfig.setReplicationStoreCommandFileNumToKeep(Integer.MAX_VALUE);
        keeperConfig.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(Integer.MAX_VALUE);
        keeperConfig.setReplicationStoreGcIntervalSeconds(1000000);
        return keeperConfig;
    }

    @Test
    public void changeStateThroughKeeperSetState() throws Exception {
        RedisMeta master = getRedisMaster();
        KeeperMeta keeperActive = activeKeeper;
        KeeperMeta keeperBackup = backupKeeper;

        SimpleObjectPool<NettyClient> masterClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(master.getIp(), master.getPort()));
        // slave repl from activeKeeper
        SimpleObjectPool<NettyClient> slaveClientPool = NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint("127.0.0.1", 8000));
        Map<KeeperMeta, SimpleObjectPool<NettyClient>> clientPoolMap = new HashMap<KeeperMeta, SimpleObjectPool<NettyClient>>() {{
            put(activeKeeper, NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(activeKeeper.getIp(), activeKeeper.getPort())));
            put(backupKeeper, NettyPoolUtil.createNettyPoolWithGlobalResource(new DefaultEndPoint(backupKeeper.getIp(), backupKeeper.getPort())));
        }};

        sendMessageToMasterAndTestSlaveRedis(10);
        Integer originFsync = infoFsync(masterClientPool);

        KeeperMeta tmp;
        int concurrent = 1000;
        for (int i = 0; i < concurrent; i++) {
            try {
                CommandChain<?> cmds = new ParallelCommandChain();
                cmds.add(new AbstractKeeperCommand.KeeperSetStateCommand(clientPoolMap.get(keeperBackup), KeeperState.ACTIVE, Pair.of(master.getIp(), master.getPort()), scheduled));
                cmds.add(new AbstractKeeperCommand.KeeperSetStateCommand(clientPoolMap.get(keeperActive), KeeperState.BACKUP, Pair.of(keeperBackup.getIp(), keeperBackup.getPort()), scheduled));
                cmds.execute().sync();
            } catch (ExecutionException e) {
                logger.info("[test] ignore setstate fail", e);
            }
            tmp = keeperBackup;
            keeperBackup = keeperActive;
            keeperActive = tmp;
        }

        sendMessageToMasterAndTestSlaveRedis(1024);

        RoleCommand masterRoleCommand = new RoleCommand(masterClientPool, scheduled);
        RoleCommand slaveRoleCommand = new RoleCommand(slaveClientPool, scheduled);
        MasterRole masterRole = (MasterRole) masterRoleCommand.execute().get();
        SlaveRole slaveRole = (SlaveRole) slaveRoleCommand.execute().get();
        Assert.assertEquals(masterRole.getOffset(), slaveRole.getMasterOffset());

        RedisKeeperServer activeKeeperServer = getRedisKeeperServer(keeperActive);
        RedisKeeperServer backupKeeperServer = getRedisKeeperServer(keeperBackup);
        Assert.assertEquals(masterRole.getOffset(), activeKeeperServer.getKeeperRepl().getEndOffset());
        Assert.assertEquals(masterRole.getOffset(), backupKeeperServer.getKeeperRepl().getEndOffset());
        Assert.assertEquals(new DefaultEndPoint(master.getIp(), master.getPort()), activeKeeperServer.getRedisMaster().masterEndPoint());
        Assert.assertEquals(new DefaultEndPoint(keeperActive.getIp(), keeperActive.getPort()), backupKeeperServer.getRedisMaster().masterEndPoint());
        Assert.assertEquals(KeeperState.ACTIVE, activeKeeperServer.getRedisKeeperServerState().keeperState());
        Assert.assertEquals(KeeperState.BACKUP, backupKeeperServer.getRedisKeeperServerState().keeperState());

        Integer fsync = infoFsync(masterClientPool);
        Assert.assertEquals(originFsync, fsync);
    }

    private int infoFsync(SimpleObjectPool<NettyClient> clientPool) throws Exception {
        InfoCommand infoCommand = new InfoCommand(clientPool, InfoCommand.INFO_TYPE.STATS, scheduled);
        return new InfoResultExtractor(infoCommand.execute().get()).extractAsInteger("sync_full");
    }

}
