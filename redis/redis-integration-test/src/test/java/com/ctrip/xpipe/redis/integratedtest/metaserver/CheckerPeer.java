package com.ctrip.xpipe.redis.integratedtest.metaserver;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.ProxyRedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.impl.XpipeRedisProxy;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

public class CheckerPeer {
    SimpleObjectPool<NettyClient> clientPool;
    ScheduledExecutorService scheduled;
    public CheckerPeer(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        this.clientPool = clientPool;
        this.scheduled = scheduled;
    }

    List<ProxyRedisMeta> getPeerMasters() {
        CRDTInfoCommand infoCommand = new CRDTInfoCommand(clientPool, InfoCommand.INFO_TYPE.REPLICATION, scheduled);
        try {
            CRDTInfoResultExtractor re = new CRDTInfoResultExtractor(infoCommand.execute().get());
            return re.extractPeerMasters();
        } catch (Exception e) {
            return null;
        }
    }
    class CheckHadPeerParams {
        Endpoint endpoint;
        CheckHadPeerParams(String ip, int port) {
            this.endpoint = new DefaultEndPoint(ip, port);
        }
    }
    CheckHadPeerParams checkHadPeerParams;
    public void setHadPeerParams(String ip, int port) {
        this.checkHadPeerParams = new CheckHadPeerParams(ip, port);
    }

    public boolean checkHadPeer() {
        List<ProxyRedisMeta> list = getPeerMasters();
        ProxyRedisMeta meta = findProxyRedisMetaByEndpoint(list, this.checkProxySizeParams.endpoint);
        if(meta == null) return false;
        return true;
    }

    class CheckProxySizeParams {
        Endpoint endpoint;
        int size;
        CheckProxySizeParams(String ip, int port, int size) {
            this.endpoint = new DefaultEndPoint(ip, port);
            this.size = size;
        }
    }

    CheckProxySizeParams checkProxySizeParams;
    public void setProxySize(String ip, int port, int proxySize) {
        this.checkProxySizeParams = new CheckProxySizeParams(ip, port, proxySize);
    }

    ProxyRedisMeta findProxyRedisMetaByEndpoint(List<ProxyRedisMeta> metas, Endpoint endpoint) {
        if(metas == null) return null;
        for(ProxyRedisMeta meta : metas) {
            if(meta.getIp().equals(endpoint.getHost()) && meta.getPort().equals(endpoint.getPort())) {
                return meta;
            }
        }
        return null;
    }
    boolean checkProxySize() {
        List<ProxyRedisMeta> list = getPeerMasters();
        ProxyRedisMeta meta = findProxyRedisMetaByEndpoint(list, this.checkProxySizeParams.endpoint);
        if(meta == null) return false;
        XpipeRedisProxy proxy = (XpipeRedisProxy)(meta.getProxy());
        return proxy.getServers().size() == this.checkProxySizeParams.size;
    }
}
