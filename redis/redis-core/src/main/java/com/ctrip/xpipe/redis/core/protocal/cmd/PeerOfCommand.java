package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxy;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.ArrayUtils;

import java.util.concurrent.ScheduledExecutorService;

public class PeerOfCommand extends AbstractRedisCommand {

    protected long gid;
    protected String ip;
    protected int port;
    protected RedisProxy proxy;

    public PeerOfCommand(SimpleObjectPool<NettyClient> clientPool, long gid, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
        this.gid = gid;
    }

    public PeerOfCommand(SimpleObjectPool<NettyClient> clientPool, long gid, String ip, int port,  ScheduledExecutorService scheduled) {
        this(clientPool, gid, ip, port, null, scheduled);
    }

    public PeerOfCommand(SimpleObjectPool<NettyClient> clientPool, long gid, String ip, int port,  RedisProxy proxy, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
        this.gid = gid;
        this.ip = ip;
        this.port = port;
        this.proxy = proxy;
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
            if(proxy == null) {
                requestString = new RequestStringParser(getName(), String.valueOf(gid), ip, String.valueOf(port));
            } else {
                String[] params = ArrayUtils.addAll(null, getName(), String.valueOf(gid), ip, String.valueOf(port));
                params = ArrayUtils.addAll(params, proxy.getParams());
                requestString = new RequestStringParser(params);
            }

        }
        return requestString.format();
    }

    @Override
    public String toString() {

        String target = getClientPool() == null? "null" : getClientPool().desc();

        if(StringUtil.isEmpty(ip)){
            return String.format("%s: %s %d no one", target, getName(), gid);
        }else {
            return String.format("%s: %s %d %s %d %s", target, getName(), gid, ip, port, proxy.toString());
        }
    }

    @Override
    protected String format(Object payload) {
        return payloadToString(payload);
    }

}
