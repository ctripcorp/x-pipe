package com.ctrip.xpipe.redis.core.protocal.cmd.proxy;

import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.impl.XpipeRedisProxy;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;


public enum RedisProxyType {

    XPIPE("xpipe_proxy") {

        public RedisProxy parse(String type, InfoResultExtractor e, int index) {
            if(!this.type.equals(type)) return null;
            String servers_str = e.extract(String.format("peer%d_proxy_servers", index));
            if(servers_str == null) { return null;}
            XpipeRedisProxy proxy = new XpipeRedisProxy();
            String[] servers = servers_str.trim().split(",");
            for( String server: servers) {
                proxy.addServer(new DefaultProxyEndpoint(String.format("PROXYTCP://%s", server.trim())));
            }
            String server_tls_str = e.extract(String.format("peer%d_proxy_tls", index));
            if(server_tls_str != null) {
                String[] tls = server_tls_str.trim().split(",");
                for( String t: tls) {
                    proxy.addTLS(new DefaultProxyEndpoint(t));
                }
            }
            return proxy;
        }
    };

    protected String type;
    RedisProxyType(String type) {
        this.type = type;
    }
    public abstract RedisProxy parse(String type, InfoResultExtractor e, int index);



};