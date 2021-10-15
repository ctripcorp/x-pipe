package com.ctrip.xpipe.redis.integratedtest.console.cmd;

import java.util.concurrent.ExecutorService;

/**
 * @author lishanglin
 * date 2021/1/25
 */
public class RedisKillCmd extends AbstractForkProcessCmd {

    private int port;

    public RedisKillCmd(int port, ExecutorService executors) {
        super(executors);
        this.port = port;
    }

    @Override
    protected void doExecute() throws Exception {
        String[] cmd = new String[] {
                "/bin/sh",
                "-c",
                String.format("kill -9 `ps -ef | grep 'redis-' | grep \"%d\" | awk '{print $2}'`", port)
        };
        execCmd(cmd);
    }

    @Override
    protected void doReset() {
    }

    @Override
    public String getName() {
        return "RedisKillCmd-" + port;
    }

}
