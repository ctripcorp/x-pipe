package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import io.netty.buffer.ByteBuf;

/**
 * @author lishanglin
 * date 2021/11/19
 */
public class TestRole implements Role {

    private Server.SERVER_ROLE role;

    public TestRole(Server.SERVER_ROLE role) {
        this.role = role;
    }

    @Override
    public Server.SERVER_ROLE getServerRole() {
        return role;
    }

    @Override
    public ByteBuf format() {
        return null;
    }
}
