package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 * <p>
 * 2016年3月29日 下午2:51:47
 */
public class Replconf extends AbstractRedisCommand<Object> {

    private ReplConfType replConfType;

    private String[] args;

    public Replconf(SimpleObjectPool<NettyClient> clientPool, ReplConfType replConfType,
                    ScheduledExecutorService scheduled, String... args) {
        super(clientPool, scheduled);
        this.replConfType = replConfType;
        this.args = args;
    }

    public Replconf(SimpleObjectPool<NettyClient> clientPool, ReplConfType replConfType,
                    ScheduledExecutorService scheduled, int commandTimeoutMilli, String... args) {
        super(clientPool, scheduled);
        this.replConfType = replConfType;
        this.args = args;
        setCommandTimeoutMilli(commandTimeoutMilli);
    }

    @Override
    public String getName() {
        return "replconf";
    }

    @Override
    protected boolean hasResponse() {

        if (replConfType == ReplConfType.ACK) {
            return false;
        }
        return true;
    }

    public enum ReplConfType {

        LISTENING_PORT("listening-port"), CAPA("capa"), CRDT("crdt"), ACK("ack"), KEEPER("keeper"), IDC("idc");

        private String command;

        ReplConfType(String command) {
            this.command = command;
        }

        @Override
        public String toString() {
            return command;
        }
    }

    @Override
    public ByteBuf getRequest() {

        boolean logRead = true, logWrite = true;

        if (replConfType == ReplConfType.ACK) {
            logWrite = false;
        }

        RequestStringParser request = null;
        if (replConfType == ReplConfType.CAPA) {
            String[] tmpArgs = new String[args.length];
            for (int i = 0; i < args.length; i++) {
                tmpArgs[i] = replConfType.toString() + " " + args[i];
            }

            request = new RequestStringParser(logRead, logWrite, getName(), StringUtil.join(" ", tmpArgs));
        } else {
            request = new RequestStringParser(logRead, logWrite, getName(), replConfType.toString(),
                    StringUtil.join(" ", args));
        }

        return request.format();
    }

    @Override
    protected boolean logRequest() {
        if (replConfType == ReplConfType.ACK) {
            return false;
        }
        return true;

    }

    @Override
    protected boolean logResponse() {
        return logRequest();
    }

    @Override
    protected Object format(Object payload) {
        return payload;
    }
}
