package com.ctrip.xpipe.redis.integratedtest.console.cmd;

import java.util.concurrent.ExecutorService;

public class CrdtRedisStartCmd extends RedisStartCmd {
    int gid;
    public CrdtRedisStartCmd(int gid, int port, ExecutorService executors) {
        super(port, false, executors);
        this.gid = gid;
    }

    protected boolean initExecuteParams() {
        if (os.startsWith("Mac")) {
            if(isMacM1()) {
                this.redisPath = "src/test/resources/redis/Mac/redis-crdt-m1";
                this.args = String.format("--loadmodule ../../resources/redis/Mac/crdt-m1.so --crdt-gid default %d", gid);
            } else {
                this.redisPath = "src/test/resources/redis/Mac/redis-crdt";
                this.args = String.format("--loadmodule ../../resources/redis/Mac/crdt.so --crdt-gid default %d", gid);
            }
            return true;
        }
        return false;
    }

    @Override
    protected void doReset() {

    }

    @Override
    public String getName() {
        return "CRDTRedisStartCmd-" + port;
    }
}

