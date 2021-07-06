package com.ctrip.xpipe.redis.core.protocal.cmd.proxy;

import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.impl.XpipeRedisProxy;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;

import java.util.Arrays;
import java.util.stream.Collectors;


public enum RedisProxyType {

    XPIPE("xpipe_proxy") {
        public RedisProxy parse(String type, InfoResultExtractor e, int index) {
            if(!this.type.equals(type)) return null;
            String servers_str = e.extract(String.format("peer%d_proxy_servers", index));
            if(servers_str == null) { return null;}
            String uri = Arrays.stream(servers_str.trim().split(",")).map(
                    server -> {
                        return "PROXYTCP://" + server;
                    }
            ).collect(Collectors.joining(","));
            String server_tls_str = e.extract(String.format("peer%d_proxy_params", index));
            uri += " " + server_tls_str;
            return XpipeRedisProxy.read(uri);
        }
    };

    protected String type;
    RedisProxyType(String type) {
        this.type = type;
    }
    public abstract RedisProxy parse(String type, InfoResultExtractor e, int index);



};