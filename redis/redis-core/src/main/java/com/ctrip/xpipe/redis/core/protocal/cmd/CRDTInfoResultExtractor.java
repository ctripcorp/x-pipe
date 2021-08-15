package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.parser.route.RouteOptionParser;
import com.ctrip.xpipe.tuple.Pair;
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

    public CRDTInfoResultExtractor(String result) {
        super(result);
    }

    public List<Pair<Long, Endpoint>> extractPeerMasters() {
        List<Pair<Long, Endpoint>> peerMasters = new LinkedList<>();

        int index = 0;
        while (true) {
            Pair<Long, Endpoint> peerMaster = tryExtractPeerMaster(index);
            if (null != peerMaster) {
                peerMasters.add(peerMaster);
            } else {
                break;
            }

            index++;
        }

        return peerMasters;
    }

    private Pair<Long, Endpoint> tryExtractPeerMaster(int index) {
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
            }
        }
        if (null == peerEndPoint)  {
            peerEndPoint = new DefaultEndPoint(host, Integer.parseInt(port));
        }
        return new Pair<>(Long.parseLong(gid), peerEndPoint);
    }

}
