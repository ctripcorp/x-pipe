package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

public class CRDTConfigGetCommand extends ConfigGetCommand<String> {
    public static String CRDT_CONFIG = "config";


    private String args;

    public CRDTConfigGetCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, String args) {
        super(clientPool, scheduled);
        this.args = args;
    }

    public CRDTConfigGetCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
        super(clientPool, scheduled, commandTimeoutMilli);
        this.args = args;
    }

    @Override
    protected String doFormat(Object[] payload) {
        if(payload.length < 2){
            throw new IllegalStateException(getName() + " result length not right:" + payload.length);
        }
        return payloadToString(payload[1]);    }

    @Override
    protected String getConfigName() {
        return args;
    }

    @Override
    public ByteBuf getRequest() {
        return new RequestStringParser(CRDT_CONFIG, "crdt.get " + getConfigName()).format();
    }

}
