package com.ctrip.xpipe.redis.console.health.redismaster;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.console.health.BaseInstanceResult;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 01, 2017
 */
public class InstanceRedisMasterResult extends BaseInstanceResult<String>{


    @Override
    public void success(long rcvNanoTime, String role) {
        super.success(rcvNanoTime, role);
    }

    public boolean roleIsMaster(){

        return Server.SERVER_ROLE.MASTER.sameRole(context);
    }

    public boolean roleIsSlave(){

        return Server.SERVER_ROLE.SLAVE.sameRole(context);
    }


    @Override
    public String toString() {
        return String.format("role：%s", context);
    }
}
