package com.ctrip.xpipe.redis.integratedtest.metaserver;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxyMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.impl.XpipeRedisProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

public class CheckerPeer {
    SimpleObjectPool<NettyClient> clientPool;
    ScheduledExecutorService scheduled;
    public CheckerPeer(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        this.clientPool = clientPool;
        this.scheduled = scheduled;
    }

    List<RedisMeta> getPeerMasters() {
        CRDTInfoCommand infoCommand = new CRDTInfoCommand(clientPool, InfoCommand.INFO_TYPE.REPLICATION, scheduled);
        try {
            CRDTInfoResultExtractor re = new CRDTInfoResultExtractor(infoCommand.execute().get());
            return re.extractPeerMasters();
        } catch (Exception e) {
            return null;
        }
    }
    class CheckHadPeerParams {
        RedisMeta meta;
        CheckHadPeerParams(Long gid, String ip, int port) {
            this.meta = new RedisMeta().setIp(ip).setPort(port).setGid(gid);
        }
        CheckHadPeerParams(RedisMeta meta) {
            this.meta = meta;
        }
    }
    CheckHadPeerParams checkHadPeerParams;
    public void setHadPeerParams(Long gid, String ip, int port) {
        this.checkHadPeerParams = new CheckHadPeerParams(gid, ip, port);
    }

    public void setHadPeerParams(RedisMeta meta) {
        this.checkHadPeerParams = new CheckHadPeerParams(meta);
    }

    public boolean checkHadPeer() {
        List<RedisMeta> list = getPeerMasters();
        RedisMeta meta = findProxyRedisMetaByMeta(list, this.checkHadPeerParams.meta);
        if(meta == null) {
            return false;
        }
        return true;
    }

    class CheckProxySizeParams {
        Endpoint endpoint;
        int size;
        CheckProxySizeParams( String ip, int port, int size) {
            this.endpoint = new DefaultEndPoint(ip, port);
            this.size = size;
        }

    }

    CheckProxySizeParams checkProxySizeParams;
    public void setProxySize(String ip, int port, int proxySize) {
        this.checkProxySizeParams = new CheckProxySizeParams(ip, port, proxySize);
    }

    RedisMeta findProxyRedisMetaByEndpoint(List<RedisMeta> metas, Endpoint m) {
        if(metas == null) return null;
        for(RedisMeta meta : metas) {
            if(meta.getIp().equals(m.getHost()) && meta.getPort().equals(m.getPort())) {
                return meta;
            }
        }
        return null;
    }

    RedisMeta findProxyRedisMetaByMeta(List<RedisMeta> metas, RedisMeta m) {
        if(metas == null) return null;
        for(RedisMeta meta : metas) {
            if(meta.getClass() == m.getClass() && meta.equals(m)) {
                return meta;
            }
        }
        return null;
    }
    Logger logger = LoggerFactory.getLogger(this.getClass());
    boolean checkProxySize() {
        List<RedisMeta> list = getPeerMasters();
        RedisMeta meta = findProxyRedisMetaByEndpoint(list, this.checkProxySizeParams.endpoint);
        if(meta == null) return false;
        if(meta instanceof RedisProxyMeta) {
            XpipeRedisProxy proxy = (XpipeRedisProxy)(((RedisProxyMeta)meta).getProxy());
            logger.info("{} == {}", proxy.getServers().size(), this.checkProxySizeParams.size);
            return proxy.getServers().size() == this.checkProxySizeParams.size;
        } else {
            logger.info("not find RedisProxyMeta");
        }
        return false;
    }
}
