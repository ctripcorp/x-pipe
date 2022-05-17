package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.utils.StringUtil;
import org.apache.logging.log4j.util.Strings;

import java.util.LinkedList;
import java.util.List;

public class CRDTInfoResultExtractor extends InfoResultExtractor {

    private static final String TEMP_PEER_HOST = "peer%d_host";
    private static final String TEMP_PEER_PORT = "peer%d_port";
    private static final String TEMP_PEER_GID = "peer%d_gid";
    private static final String TEMP_PROXY_TYPE = "peer%d_proxy_type";
    private static final String TEMP_PROXY_SERVERS = "peer%d_proxy_servers";
    private static final String TEMP_PROXY_PARAMS = "peer%d_proxy_params";
    private static final String TEMP_REPL_OFFSET = "peer%d_repl_offset";
    
    public CRDTInfoResultExtractor(String result) {
        super(result);
    }
    
    public static class PeerInfo {
        private Endpoint endpoint;
        private long replOffset;
        long gid;
        PeerInfo(long gid, Endpoint endpoint) {
            this.gid = gid;
            this.endpoint = endpoint;
        }

        public PeerInfo setReplOffset(long replOffset) {
            this.replOffset = replOffset;
            return this;
        }

        public long getReplOffset() {
            return replOffset;
        }

        public Endpoint getEndpoint() {
            return endpoint;
        }

        public long getGid() {
            return gid;
        }
    }

    public List<PeerInfo> extractPeerMasters() {
        List<PeerInfo> peerMasters = new LinkedList<>();

        int index = 0;
        while (true) {
            PeerInfo peerMaster = tryExtractPeerMaster(index);
            if (null != peerMaster) {
                peerMasters.add(peerMaster);
            } else {
                break;
            }

            index++;
        }

        return peerMasters;
    }
    
    private PeerInfo tryExtractPeerMaster(int index) {
        String host = extract(String.format(TEMP_PEER_HOST, index));
        String port = extract(String.format(TEMP_PEER_PORT, index));
        String gid = extract(String.format(TEMP_PEER_GID, index));

        if (null == host || null == port) return null;
        Endpoint peerEndPoint = null;
        String proxyType = extract(String.format(TEMP_PROXY_TYPE, index));
        if (!Strings.isEmpty(proxyType)) {
            switch (proxyType) {
                case PeerOfCommand.TYPE_XPIPE_PROXY:
                    String servers = extract(String.format(TEMP_PROXY_SERVERS, index));
                    String protocolStr = String.format("%s %s %s",ProxyConnectProtocol.KEY_WORD, PROXY_OPTION.ROUTE, servers);
                    String params = extract(String.format(TEMP_PROXY_PARAMS, index));
                    if(!StringUtil.isEmpty(params)) {
                        protocolStr += " " + params;
                    }
                    ProxyConnectProtocol protocol = new DefaultProxyConnectProtocolParser().read(protocolStr);
                    peerEndPoint = new ProxyEnabledEndpoint(host, Integer.parseInt(port), protocol);
                    break;
                default:
                    logger.warn("[UnKnow CRDT Redis Proxy Protocol type] {}", proxyType);
            }
        }
        if (null == peerEndPoint)  {
            peerEndPoint = new DefaultEndPoint(host, Integer.parseInt(port));
        }
        PeerInfo info =  new PeerInfo(Long.parseLong(gid), peerEndPoint);
        long replOffset = extractAsLong(String.format(TEMP_REPL_OFFSET, index));
        info.setReplOffset(replOffset);
        return info;
    }
}
