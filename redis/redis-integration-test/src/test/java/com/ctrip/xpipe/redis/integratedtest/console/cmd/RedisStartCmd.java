package com.ctrip.xpipe.redis.integratedtest.console.cmd;

import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;

import java.util.concurrent.ExecutorService;

/**
 * @author lishanglin
 * date 2021/1/21
 */
public class RedisStartCmd extends AbstractForkProcessCmd {

    protected int port;

    private boolean asSentinel;

    protected String os;

    protected String redisPath;

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
    /**
     *  0： version1 == version2
     *  1:  version1 > version2 (1.0.2 > 1.0.1, 1.0.1.1 > 1.0.1)
     *  -1: version1 < version2
     */
    public static int compareVersion(String version1, String version2) {
        if (version1.equals(version2)) {
            return 0;
        }
        String[] v1Array = version1.split("\\.");
        String[] v2Array = version2.split("\\.");
        int v1Len = v1Array.length;
        int v2Len = v2Array.length;
        int baseLen = 0;
        if(v1Len > v2Len){
            baseLen = v2Len;
        }else{
            baseLen = v1Len;
        }

        for(int i=0;i<baseLen;i++){
            if(v1Array[i].equals(v2Array[i])){
                continue;
            }else{
                return Integer.parseInt(v1Array[i])>Integer.parseInt(v2Array[i]) ? 1 : -1;
            }
        }
        if(v1Len != v2Len){
            return v1Len > v2Len ? 1:-1;
        }else {
            return 0;
        }
    }

    protected boolean isMacM1() {
        return compareVersion(System.getProperty("os.version"), "11.4") >= 0;
    }

    protected boolean initExecuteParams() {
        if (os.startsWith("Mac")) {
            if(isMacM1()) {
                //m1
                redisPath = "src/test/resources/redis/Mac/redis-server-m1";
            } else {
                redisPath = "src/test/resources/redis/Mac/redis-server";
            }
            return true;
        }
        return false;
    }

    @Override
    protected void doExecute() throws Exception {
        if(redisPath == null) {
             if(!initExecuteParams()) {
                throw new RedisRuntimeException("not support this system temporarily");
            }
        }
        execServer();
    }
    protected void execServer() throws Exception {
        if (null == redisPath) {
            future().setFailure(new IllegalArgumentException("no redis-server for os " + os));
        } else {
            String url = String.format("./src/test/tmp/redis%d", port);

            execCmd(new String[]{
                    "/bin/sh",
                    "-c",
                    String.format("mkdir -p src/test/tmp;" +
                                    "mkdir -p %s;" +
                                    "rm -f %s/dump.rdb;" +
                                    "rm -f %s/redis.conf;" +
                                    "touch %s/redis.conf;" +
                                    "%s %s/redis.conf --port %d --logfile redis.log %s %s ",
                            url, url, url, url, redisPath, url, port,
                            asSentinel ? "--sentinel"
                                    : String.format("--dir %s --dbfilename dump.rdb --repl-backlog-size 100mb --appendonly no", url),
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
