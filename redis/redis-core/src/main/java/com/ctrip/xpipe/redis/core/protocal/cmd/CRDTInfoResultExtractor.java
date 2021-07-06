package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxyMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxy;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxyFactory;
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

    public List<RedisMeta> extractPeerMasters() {
        List<RedisMeta> peerMasters = new LinkedList<>();

        int index = 0;
        while (true) {
            RedisMeta peerMaster = tryExtractPeerMaster(index);
            if (null != peerMaster) {
                peerMasters.add(peerMaster);
            } else {
                break;
            }

            index++;
        }

        return peerMasters;
    }

    private RedisMeta tryExtractPeerMaster(int index) {
        String host = extract(String.format(TEMP_PEER_HOST, index));
        String port = extract(String.format(TEMP_PEER_PORT, index));
        String gid = extract(String.format(TEMP_PEER_GID, index));

        if (null == host || null == port) return null;
        RedisProxy proxy = RedisProxyFactory.valueofInfo(this, index);
        RedisMeta peerMaster;
        if(proxy == null) {
            peerMaster = new RedisMeta();
        } else {
            peerMaster = new RedisProxyMeta().setProxy(proxy);
        }
        peerMaster.setIp(host).setPort(Integer.parseInt(port));

        if (!StringUtil.isEmpty(gid)) {
            // allow gid missing for low version crdt redis
            peerMaster.setGid(Long.parseLong(gid));
        }

        return peerMaster;
    }

}
