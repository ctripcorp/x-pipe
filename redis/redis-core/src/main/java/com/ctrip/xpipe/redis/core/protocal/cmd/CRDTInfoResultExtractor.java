package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.ProxyRedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxy;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxyFactory;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxyType;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.LinkedList;
import java.util.List;

public class CRDTInfoResultExtractor extends InfoResultExtractor {

    private static final String TEMP_PEER_HOST = "peer%d_host";
    private static final String TEMP_PEER_PORT = "peer%d_port";
    private static final String TEMP_PEER_GID = "peer%d_gid";


    public CRDTInfoResultExtractor(String result) {
        super(result);
    }

    ProxyRedisMeta tryExtractPeerRedisMaster(int index) {
        ProxyRedisMeta redis = tryExtractPeerMaster(index);
        if(redis == null) return null;
        RedisProxy proxy = RedisProxyFactory.valueofInfo(this, index);
        return redis.setProxy(proxy);
    }

    public List<ProxyRedisMeta> extractPeerMasters() {
        List<ProxyRedisMeta> peerRedisMetas = new LinkedList<>();

        int index = 0;
        while(true) {
            ProxyRedisMeta peerMaster = tryExtractPeerRedisMaster(index);
            if (null != peerMaster) {
                peerRedisMetas.add(peerMaster);
            } else {
                break;
            }
            index++;
        }
        return peerRedisMetas;
    }

    private ProxyRedisMeta tryExtractPeerMaster(int index) {
        String host = extract(String.format(TEMP_PEER_HOST, index));
        String port = extract(String.format(TEMP_PEER_PORT, index));
        String gid = extract(String.format(TEMP_PEER_GID, index));

        if (null == host || null == port) return null;

        ProxyRedisMeta peerMaster = (ProxyRedisMeta)new ProxyRedisMeta().setIp(host).setPort(Integer.parseInt(port));

        if (!StringUtil.isEmpty(gid)) {
            // allow gid missing for low version crdt redis
            peerMaster.setGid(Long.parseLong(gid));
        }

        return peerMaster;
    }

}
