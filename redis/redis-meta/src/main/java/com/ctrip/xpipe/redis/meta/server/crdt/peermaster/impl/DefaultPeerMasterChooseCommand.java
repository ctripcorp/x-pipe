package com.ctrip.xpipe.redis.meta.server.crdt.peermaster.impl;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DefaultPeerMasterChooseCommand extends AbstractPeerMasterChooseCommand {

    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    private int checkRedisTimeoutSeconds;

    protected DcMetaCache dcMetaCache;

    protected ScheduledExecutorService scheduled;

    public DefaultPeerMasterChooseCommand(String dcId, String clusterId, String shardId, DcMetaCache dcMetaCache, ScheduledExecutorService scheduled,
                                          XpipeNettyClientKeyedObjectPool keyedObjectPool, int checkRedisTimeoutSeconds) {
        super(dcId, clusterId, shardId);
        this.dcMetaCache = dcMetaCache;
        this.scheduled = scheduled;
        this.keyedObjectPool = keyedObjectPool;
        this.checkRedisTimeoutSeconds = checkRedisTimeoutSeconds;
    }

    @Override
    public RedisMeta choose() throws Exception {
        List<RedisMeta> allRedises = dcMetaCache.getShardRedises(clusterId, shardId);

        List<RedisMeta> redisMasters = getMasters(allRedises);

        if (1 == redisMasters.size()) {
            RedisMeta peerMaster = redisMasters.get(0);
            peerMaster.setGid(getRedisGid(peerMaster.getIp(), peerMaster.getPort()));
            return peerMaster;
        } else {
            getLogger().info("[choose] unexpected master size {} for {}, {}, masters: {}",
                    redisMasters.size(), clusterId, shardId, redisMasters);
        }

        return null;
    }

    protected List<RedisMeta> getMasters(List<RedisMeta> allRedises) {

        List<RedisMeta> result = new LinkedList<>();

        for(RedisMeta redisMeta : allRedises){
            if(isMaster(redisMeta)) {
                result.add(redisMeta);
            }
        }

        return result;
    }

    protected boolean isMaster(RedisMeta redisMeta) {

        try {
            SimpleObjectPool<NettyClient> clientPool = keyedObjectPool.getKeyPool(new DefaultEndPoint(redisMeta.getIp(), redisMeta.getPort()));
            Role role = new RoleCommand(clientPool, checkRedisTimeoutSeconds*1000, false, scheduled).execute().get(checkRedisTimeoutSeconds, TimeUnit.SECONDS);
            return Server.SERVER_ROLE.MASTER == role.getServerRole();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            getLogger().error("[isMaster]" + redisMeta, e);
        }
        return false;
    }

    protected int getRedisGid(String ip, int port) throws Exception {
        try {
            SimpleObjectPool<NettyClient> clientPool = keyedObjectPool.getKeyPool(new DefaultEndPoint(ip, port));
            String infoStr = new CRDTInfoCommand(clientPool, InfoCommand.INFO_TYPE.REPLICATION, scheduled).execute().get(checkRedisTimeoutSeconds, TimeUnit.SECONDS);
            InfoResultExtractor extractor = new InfoResultExtractor(infoStr);
            String rawGid = extractor.extract("gid");
            if (null == rawGid) {
                throw new IllegalStateException(String.format("no info gid found for cluster %s shard %s",  clusterId, shardId));
            }

            int gid = Integer.parseInt(rawGid);
            if (gid > 0) {
                return gid;
            } else {
                throw new IllegalStateException(String.format("unexpected gid %s for cluster %s shard %s", rawGid, clusterId, shardId));
            }
        } catch (Exception e) {
            getLogger().error("[getRedisGid] {}, {}", ip, port, e);
            throw e;
        }
    }

}
