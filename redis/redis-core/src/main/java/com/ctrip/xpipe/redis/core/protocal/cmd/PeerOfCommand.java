package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxy;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxyMeta;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.ArrayUtils;

import java.util.concurrent.ScheduledExecutorService;

public class PeerOfCommand extends AbstractRedisCommand {

    protected long gid;
    protected RedisMeta redisMeta;

    public PeerOfCommand(SimpleObjectPool<NettyClient> clientPool, long gid, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
        this.gid = gid;
    }

    public PeerOfCommand(SimpleObjectPool<NettyClient> clientPool, long gid, String ip, int port,  ScheduledExecutorService scheduled) {
        this(clientPool, gid, ip, port, null, scheduled);
    }

    public PeerOfCommand(SimpleObjectPool<NettyClient> clientPool, long gid, RedisMeta meta, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
        this.gid = gid;
        this.redisMeta = meta;
    }

    public PeerOfCommand(SimpleObjectPool<NettyClient> clientPool, long gid, String ip, int port,  RedisProxy proxy, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
        this.gid = gid;
        if(proxy == null) {
            this.redisMeta = new RedisMeta();
        } else {
            this.redisMeta = new RedisProxyMeta().setProxy(proxy);
        }
        this.redisMeta.setGid(gid).setIp(ip).setPort(port);
    }

    @Override
    public String getName() {
        return "peerof";
    }

    @Override
    public ByteBuf getRequest() {

        RequestStringParser requestString = null;
        if(redisMeta == null){
            requestString = new RequestStringParser(getName(), String.valueOf(gid), "no", "one");
        }else{
            String[] params = ArrayUtils.addAll(null, getName(), String.valueOf(gid), redisMeta.getIp(), String.valueOf(redisMeta.getPort()));
            if(redisMeta instanceof RedisProxyMeta) {
                if(((RedisProxyMeta) redisMeta).getProxy() != null) {
                    params = ArrayUtils.addAll(params, ((RedisProxyMeta) redisMeta).getProxy().getRequest());
                }
            }
            requestString = new RequestStringParser(params);
        }
        return requestString.format();
    }

    @Override
    public String toString() {

        String target = getClientPool() == null? "null" : getClientPool().desc();

        if(redisMeta == null){
            return String.format("%s: %s %d no one", target, getName(), gid);
        }else {
            return String.format("%s: %s %d %s ", target, getName(), gid, redisMeta.toString());
        }
    }

    @Override
    protected String format(Object payload) {
        return payloadToString(payload);
    }

}
