package com.ctrip.xpipe.redis.integratedtest.console.cmd;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.integratedtest.console.ForkProcessCmd;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * @author lishanglin
 * date 2021/1/22
 */
public abstract class AbstractForkProcessCmd extends AbstractCommand<Integer> implements ForkProcessCmd {

    private Process process;

    private ExecutorService executors;

    public AbstractForkProcessCmd(ExecutorService executors) {
        this.executors = executors;
    }

    public void killProcess() {
        if (null != process) {
            process.destroy();
            process = null;
        }
    }

    public boolean isProcessAlive() {
        return null != process && process.isAlive();
    }

    protected void execCmd(String[] cmd) throws Exception {
        getLogger().info("[execCmd] cmd: {}", String.join(" ", cmd));
        process = Runtime.getRuntime().exec(cmd);

        logProcess();
    }

    protected void execCmd(String cmd) throws Exception {
        getLogger().info("[execCmd] cmd: {}", cmd);
        process = Runtime.getRuntime().exec(cmd);

        logProcess();
    }

    private void logProcess() throws Exception {
        // log redirect
        executors.submit(new StreamGobbler(process.getInputStream(), this::log));
        executors.submit(new StreamGobbler(process.getErrorStream(), this::log));

        future().setSuccess(process.waitFor());
    }

    protected void log(String info) {
        getLogger().info("[{}]{}", getName(), info);
    }

    private static class StreamGobbler implements Runnable {
        private InputStream inputStream;
        private Consumer<String> consumer;

        public StreamGobbler(InputStream inputStream, Consumer<String> consumer) {
            this.inputStream = inputStream;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines()
                    .forEach(consumer);
        }
    }

}
