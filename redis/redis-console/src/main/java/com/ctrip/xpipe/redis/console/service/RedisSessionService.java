package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.healthcheck.fulllink.model.RedisRoleModel;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;

import java.util.List;


public interface RedisSessionService {

    CommandFuture<Role> getRedisRole(String ip, int port, RedisSession.RollCallback callback);

    List<RedisRoleModel> getShardAllRedisRole( String dcId, String clusterId, String shardId);

}
