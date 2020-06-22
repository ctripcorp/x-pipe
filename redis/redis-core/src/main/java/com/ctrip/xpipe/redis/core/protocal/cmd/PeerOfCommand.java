package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

public class PeerOfCommand extends AbstractRedisCommand {

    protected long gid;
    protected String ip;
    protected int port;

    public PeerOfCommand(SimpleObjectPool<NettyClient> clientPool, long gid, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
        this.gid = gid;
    }

    public PeerOfCommand(SimpleObjectPool<NettyClient> clientPool, long gid, String ip, int port, ScheduledExecutorService scheduled){
        this(clientPool, gid, ip, port, "", scheduled);
    }

    public PeerOfCommand(SimpleObjectPool<NettyClient> clientPool, long gid, String ip, int port, String param, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
        this.gid = gid;
        this.ip = ip;
        this.port = port;
    }

    @Override
    public String getName() {
        return "peerof";
    }

    @Override
    public ByteBuf getRequest() {

        RequestStringParser requestString = null;
        if(StringUtil.isEmpty(ip)){
            requestString = new RequestStringParser(getName(), String.valueOf(gid), "no", "one");
        }else{
            requestString = new RequestStringParser(getName(), String.valueOf(gid), ip, String.valueOf(port));
        }
        return requestString.format();
    }

    @Override
    public String toString() {

        String target = getClientPool() == null? "null" : getClientPool().desc();

        if(StringUtil.isEmpty(ip)){
            return String.format("%s: %s %d no one", target, getName(), gid);
        }else{
            return String.format("%s: %s %d %s %d", target, getName(), gid, ip, port);
        }
    }

    @Override
    protected String format(Object payload) {
        return payloadToString(payload);
    }

}
