package com.ctrip.xpipe.redis.meta.server.crdt.master.command;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxyMeta;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;


public class PeerMasterChooseCommand extends AbstractMasterChooseCommand {

    private String dcId;

    private MultiDcService multiDcService;

    public PeerMasterChooseCommand(String dcId, String clusterId, String shardId, MultiDcService multiDcService) {
        super(clusterId, shardId);
        this.dcId = dcId;
        this.multiDcService = multiDcService;
    }

    @Override
    public RedisMeta choose() {
        RedisMeta redisMeta = multiDcService.getPeerMaster(dcId, clusterId, shardId);
        RouteMeta routeMeta = multiDcService.getRouteMeta(dcId, clusterId);
        if(routeMeta != null) {
            redisMeta = RedisProxyMeta.create(redisMeta, routeMeta);
        }
        return redisMeta;
    }

}
