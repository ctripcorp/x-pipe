package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.KeeperStateModel;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;

import java.util.List;

public interface KeeperSessionService {

    String PATH_KEEPER_INFO_PORT = "/keepers/port/{port}";

    CommandFuture<String> infoKeeper(String ip, int port, String infoSection, Callbackable<String> callback);

    CommandFuture<Role> getKeeperRole(String ip, int port, RedisSession.RollCallback callback);

    KeeperInstanceMeta getKeeperReplId(String ip, int port);

    List<KeeperStateModel>  getShardAllKeeperState(String dcId, String clusterId, String shardId);

}
