package com.ctrip.xpipe.redis.integratedtest.console.cmd;

import java.util.concurrent.ExecutorService;

/**
 * @author lishanglin
 * date 2021/1/21
 */
public class RedisStartCmd extends AbstractForkProcessCmd {

    protected int port;

    protected boolean asSentinel;

    protected String os;

    protected String arch;

    protected String args = "";

    public RedisStartCmd(int port, ExecutorService executors) {
        this(port, false, executors);
    }

    public RedisStartCmd(int port, boolean asSentinel, ExecutorService executors) {
        super(executors);
        this.port = port;
        this.asSentinel = asSentinel;
        this.os = System.getProperty("os.name");
        this.arch = System.getProperty("os.arch");
    }

    @Override
    protected void doExecute() throws Exception {
        String redisPath = null;
        if (os.startsWith("Mac")) {
            redisPath = "src/test/resources/redis/Mac/redis-server";
        }
        execServer(redisPath);
    }

    protected void execServer(String redisPath) throws Exception {
        if (null == redisPath) {
            future().setFailure(new IllegalArgumentException("no redis-server for os " + os));
        } else {
            execCmd(new String[]{
                    "/bin/sh",
                    "-c",
                    String.format("mkdir -p src/test/tmp;" +
                            "mkdir -p src/test/tmp/redis%d;" +
                            "%s --port %d  --dir src/test/tmp/redis%d --logfile redis.log %s %s",
                            port,redisPath, port, port,
                            asSentinel ? "--sentinel"
                                    : String.format("--dbfilename dump%d.rdb --repl-backlog-size 100mb --appendonly no", port),
                            args)
            });
        }
    }


    @Override
    protected void doReset() {
    }

    @Override
    public String getName() {
        return "RedisStartCmd-" + port;
    }

}
