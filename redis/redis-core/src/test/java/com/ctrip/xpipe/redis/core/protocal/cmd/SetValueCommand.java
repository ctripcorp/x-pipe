package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

public class SetValueCommand extends AbstractRedisCommand<String> {

    String key;
    String value;

    public SetValueCommand(String host, int port, ScheduledExecutorService scheduled, String key, String value) {
        super(host, port, scheduled);
        this.key = key;
        this.value = value;
    }

    @Override
    public ByteBuf getRequest() {
        RequestStringParser requestString = null;
        requestString = new RequestStringParser(getName(), key, value);
        return requestString.format();
    }

    @Override
    protected String format(Object payload) {
        return payloadToString(payload);
    }

    @Override
    public String getName() {
        return "set";
    }

}
