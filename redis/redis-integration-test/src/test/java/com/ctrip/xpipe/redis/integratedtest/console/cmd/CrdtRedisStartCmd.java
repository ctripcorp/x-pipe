package com.ctrip.xpipe.redis.integratedtest.console.cmd;

import java.util.concurrent.ExecutorService;

public class CrdtRedisStartCmd extends RedisStartCmd {
    int gid;
    public CrdtRedisStartCmd(int gid, int port, ExecutorService executors) {
        super(port, false, executors);
        this.gid = gid;
    }

    @Override
    protected void doExecute() throws Exception {
        String redisPath = null;
        if (os.startsWith("Mac")) {
            redisPath = "src/test/resources/redis/Mac/redis-crdt";
        }
        this.args = String.format("--loadmodule ../../resources/redis/Mac/crdt.so --crdt-gid default %d", gid);
        execServer(redisPath);
    }

    @Override
    protected void doReset() {

    }

    @Override
    public String getName() {
        return "CRDTRedisStartCmd-" + port;
    }
}
