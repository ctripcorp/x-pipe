package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.parser.route.RouteOptionParser;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.ArrayUtils;

import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class PeerOfCommand extends AbstractRedisCommand {

    protected long gid;
    protected Endpoint endpoint;

    public PeerOfCommand(SimpleObjectPool<NettyClient> clientPool, long gid, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
        this.gid = gid;
    }

    public PeerOfCommand(SimpleObjectPool<NettyClient> clientPool, long gid, Endpoint endpoint, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
        this.gid = gid;
        this.endpoint = endpoint;
    }

    @Override
    public String getName() {
        return "peerof";
    }

    static final String TEMP_PROXY_TYPE = "proxy-type";
    static final String TEMP_PROXY_SERVERS = "proxy-servers";
    static final String TEMP_PROXY_PARAMS = "proxy-params";
    static final String TYPE_XPIPE_PROXY = "XPIPE-PROXY";
    @Override
    public ByteBuf getRequest() {

        RequestStringParser requestString;
        if(null == endpoint){
            requestString = new RequestStringParser(getName(), String.valueOf(gid), "no", "one");
        }else{
            String[] params = ArrayUtils.addAll(null, getName(), String.valueOf(gid), endpoint.getHost(), String.valueOf(endpoint.getPort()));
            ProxyConnectProtocol protocol = endpoint.getProxyProtocol();
            if(protocol != null) {
                RouteOptionParser parser = new RouteOptionParser().read(PROXY_OPTION.ROUTE + " " + protocol.getRouteInfo());
                String servers = parser.getNextEndpoints().stream().map(endpoint-> endpoint.toString()).collect(Collectors.joining(","));
                params = ArrayUtils.addAll(params, TEMP_PROXY_TYPE, TYPE_XPIPE_PROXY, TEMP_PROXY_SERVERS, servers);
                String proxyParams = new RouteOptionParser().read(parser.output()).getContent();
                if(proxyParams != null) {
                    params = ArrayUtils.addAll(params, TEMP_PROXY_PARAMS, "\"" + proxyParams + "\"");
                }
            }
            requestString = new RequestStringParser(params);
        }
        return requestString.format();
    }

    @Override
    public String toString() {

        String target = getClientPool() == null? "null" : getClientPool().desc();

        if(null == endpoint){
            return String.format("%s: %s %d no one", target, getName(), gid);
        }else{
            return String.format("%s: %s %d %s %d %s", target, getName(), gid, endpoint.getHost(), endpoint.getPort(), endpoint.getProxyProtocol());
        }
    }

    @Override
    protected String format(Object payload) {
        return payloadToString(payload);
    }
    
}
