package com.ctrip.xpipe.redis.integratedtest.dr.cmd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * @author lishanglin
 * date 2021/1/21
 */
public class RedisStartCmd extends AbstractForkProcessCmd {

    private int port;

    private String os;

    private String arch;

    public RedisStartCmd(int port, ExecutorService executors) {
        super(executors);
        this.port = port;
        this.os = System.getProperty("os.name");
        this.arch = System.getProperty("os.arch");
    }

    @Override
    protected void doExecute() throws Exception {
        String redisPath = null;
        if (os.startsWith("Linux") || arch.startsWith("arm")) {
            redisPath = "src/test/resources/redis/Linux/redis-server";
        } else if (os.startsWith("Mac")) {
            redisPath = "src/test/resources/redis/Mac/redis-server";
        }

        if (null == redisPath) {
            future().setFailure(new IllegalArgumentException("no redis-server for os " + os));
        } else {
            execCmd(new String[]{
                    "/bin/sh",
                    "-c",
                    String.format("mkdir -p src/test/tmp;" +
                            "touch src/test/tmp/redis%d.conf;" +
                            "%s src/test/tmp/redis%d.conf --port %d --dir src/test/tmp --dbfilename dump%d.rdb --repl-backlog-size 100mb --appendonly no",
                            port, redisPath, port, port, port)
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
