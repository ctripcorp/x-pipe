package com.ctrip.xpipe.redis.meta.server.crdt.master.impl;

import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxyMeta;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooseCommandFactory;
import com.ctrip.xpipe.redis.meta.server.crdt.master.command.CurrentMasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.crdt.master.command.PeerMasterChooseCommand;
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
    public MasterChooseCommand buildPeerMasterChooserCommand(String dcId, String clusterId, String shardId) {
        MasterChooseCommand masterChooseCommand = new PeerMasterChooseCommand(dcId, clusterId, shardId, multiDcService);
        return wrapPeerMasterChooseCommand(dcId, clusterId, shardId, masterChooseCommand);
    }

    protected MasterChooseCommand wrapPeerMasterChooseCommand(String dcId, String clusterId, String shardId, MasterChooseCommand command) {
        command.future().addListener(commandFuture -> {
            logger.debug("[peerMasterChooseComplete]{}, {}, {}", dcId, clusterId, shardId);
            if (commandFuture.isSuccess()) {
                RedisMeta master = (RedisMeta)commandFuture.get();
                RedisMeta currentMaster = currentMetaManager.getPeerMaster(dcId, clusterId, shardId);

                if (checkMasterChange(master, currentMaster)) {
                    logger.info("[operationComplete][setPeerMaster]{}, {}, {}, {}}", dcId, clusterId, shardId, master);
                    currentMetaManager.setPeerMaster(dcId, clusterId, shardId, master);
                }
            } else {
               logger.error("[wrapPeerMasterChooseCommand] commandFuture fail: {}", commandFuture.cause());
            }
        });

        return command;
    }

    @Override
    public MasterChooseCommand buildCurrentMasterChooserCommand(String clusterId, String shardId) {
        List<RedisMeta> redisMetas =  dcMetaCache.getShardRedises(clusterId, shardId);
        MasterChooseCommand masterChooseCommand = new CurrentMasterChooseCommand(clusterId, shardId,
                redisMetas, scheduled, keyedObjectPool, PEER_MASTER_CHECK_REDIS_TIMEOUT_SECONDS);
        return wrapCurrentMasterChooseCommand(clusterId, shardId, masterChooseCommand);
    }

    protected MasterChooseCommand wrapCurrentMasterChooseCommand(String clusterId, String shardId, MasterChooseCommand command) {
        command.future().addListener(commandFuture -> {
            logger.debug("[currentMasterChooseComplete]{}, {}", clusterId, shardId);
            if (commandFuture.isSuccess()) {
                RedisMeta master = commandFuture.get();
                RedisMeta currentMaster = currentMetaManager.getCurrentCRDTMaster(clusterId, shardId);

                if (checkMasterChange(master, currentMaster)) {
                    logger.info("[operationComplete][setCurrentMaster]{}, {}, {}", clusterId, shardId, master);
                    currentMetaManager.setCurrentCRDTMaster(clusterId, shardId, master);
                }
            } else {
                logger.error("[wrapCurrentMasterChooseCommand] commandFuture fail: {}", commandFuture.cause());
            }
        });

        return command;
    }

    private boolean checkMasterChange(RedisMeta newMaster, RedisMeta currentMaster) {
        if (null == newMaster) {
            return false;
        }
        if ((null != currentMaster
                && newMaster.getClass().equals(currentMaster.getClass())
                && newMaster.getGid().equals(currentMaster.getGid())
                && newMaster.getIp().equals(currentMaster.getIp())
                && newMaster.getPort().equals(currentMaster.getPort()))) {
            if(newMaster instanceof RedisProxyMeta) {
                if(((RedisProxyMeta)newMaster).getProxy().equals(((RedisProxyMeta)currentMaster).getProxy())) {
                    logger.debug("[checkMasterChange][new master null or equals to old master]{}", newMaster);
                    return false;
                }
                return true;
            }
            return false;

        }
        return true;
    }

}
