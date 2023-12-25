package com.ctrip.xpipe.redis.meta.server.crdt.master.impl;

import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooseCommandFactory;
import com.ctrip.xpipe.redis.meta.server.crdt.master.command.CurrentMasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.crdt.master.command.PeerMasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.crdt.master.command.RedundantMasterClearCommand;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig.CLIENT_POOL;

@Component
public class DefaultMasterChooseCommandFactory implements MasterChooseCommandFactory {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected DcMetaCache dcMetaCache;

    protected CurrentMetaManager currentMetaManager;

    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    private MultiDcService multiDcService;

    protected ScheduledExecutorService scheduled;

    public static final int PEER_MASTER_CHECK_REDIS_TIMEOUT_SECONDS = Integer
            .parseInt(System.getProperty("PEER_MASTER_CHECK_REDIS_TIMEOUT_SECONDS", "1"));

    @Autowired
    public DefaultMasterChooseCommandFactory(DcMetaCache dcMetaCache, CurrentMetaManager currentMetaManager,
                                             @Qualifier(CLIENT_POOL) XpipeNettyClientKeyedObjectPool keyedObjectPool,
                                             MultiDcService multiDcService) {
        this.dcMetaCache = dcMetaCache;
        this.currentMetaManager = currentMetaManager;
        this.keyedObjectPool = keyedObjectPool;
        this.multiDcService = multiDcService;

        scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("PeerMasterChooseCommandSchedule"));
    }

    @Override
    public RedundantMasterClearCommand buildRedundantMasterClearCommand(Long clusterDbId, Long shardDbId, Set<String> dcs) {
        return new RedundantMasterClearCommand(clusterDbId, shardDbId, dcs, currentMetaManager);
    }

    @Override
    public MasterChooseCommand buildPeerMasterChooserCommand(String dcId, Long clusterDbId, Long shardDbId) {
        MasterChooseCommand masterChooseCommand = new PeerMasterChooseCommand(dcId, clusterDbId, shardDbId, multiDcService);
        return wrapPeerMasterChooseCommand(dcId, clusterDbId, shardDbId, masterChooseCommand);
    }

    protected MasterChooseCommand wrapPeerMasterChooseCommand(String dcId, Long clusterDbId, Long shardDbId, MasterChooseCommand command) {
        command.future().addListener(commandFuture -> {
            logger.debug("[peerMasterChooseComplete]{}, cluster_{}, shard_{}", dcId, clusterDbId, shardDbId);
            if (commandFuture.isSuccess()) {
                RedisMeta master = commandFuture.get();
                RedisMeta currentMaster = currentMetaManager.getPeerMaster(dcId, clusterDbId, shardDbId);

                if (checkMasterChange(master, currentMaster)) {
                    logger.info("[operationComplete][setPeerMaster]{}, cluster_{}, shard_{}, {}", dcId, clusterDbId, shardDbId, master);
                    currentMetaManager.setPeerMaster(dcId, clusterDbId, shardDbId, master.getGid(), master.getIp(), master.getPort());
                }
            }
        });

        return command;
    }

    @Override
    public MasterChooseCommand buildCurrentMasterChooserCommand(Long clusterDbId, Long shardDbId) {
        List<RedisMeta> redisMetas =  dcMetaCache.getShardRedises(clusterDbId, shardDbId);
        MasterChooseCommand masterChooseCommand = new CurrentMasterChooseCommand(clusterDbId, shardDbId,
                redisMetas, scheduled, keyedObjectPool, PEER_MASTER_CHECK_REDIS_TIMEOUT_SECONDS);
        return wrapCurrentMasterChooseCommand(clusterDbId, shardDbId, masterChooseCommand);
    }

    protected MasterChooseCommand wrapCurrentMasterChooseCommand(Long clusterDbId, Long shardDbId, MasterChooseCommand command) {
        command.future().addListener(commandFuture -> {
            logger.debug("[currentMasterChooseComplete]cluster_{}, shard_{}", clusterDbId, shardDbId);
            if (commandFuture.isSuccess()) {
                RedisMeta master = commandFuture.get();
                RedisMeta currentMaster = currentMetaManager.getCurrentCRDTMaster(clusterDbId, shardDbId);

                if (checkMasterChange(master, currentMaster)) {
                    logger.info("[operationComplete][setCurrentMaster]cluster_{}, shard_{}, {}", clusterDbId, shardDbId, master);
                    currentMetaManager.setCurrentCRDTMaster(clusterDbId, shardDbId, master.getGid(), master.getIp(), master.getPort());
                }
            }
        });

        return command;
    }

    private boolean checkMasterChange(RedisMeta newMaster, RedisMeta currentMaster) {
        if (null == newMaster || (null != currentMaster
                && newMaster.getGid().equals(currentMaster.getGid())
                && newMaster.getIp().equals(currentMaster.getIp())
                && newMaster.getPort().equals(currentMaster.getPort()))) {
            logger.debug("[checkMasterChange][new master null or equals to old master]{}", newMaster);
            return false;
        }

        return true;
    }

}
