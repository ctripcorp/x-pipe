package com.ctrip.xpipe.redis.core.proxy.parser.ping;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyProtocolResponse;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyReqResOptionParser;
import io.netty.channel.Channel;

import java.net.InetSocketAddress;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2018
 */
public class PingOptionParser extends AbstractProxyOptionParser implements ProxyReqResOptionParser {

    private static final String PING = "PING";

    private FoundationService foundation = FoundationService.DEFAULT;

    private Channel channel;

    public PingOptionParser(Channel channel) {
        this.channel = channel;
    }

    @Override
    public ProxyProtocolResponse getResponse() {
        return new ProxyPingResponse(foundation.getLocalIp(), ((InetSocketAddress)channel.localAddress()).getPort());
    }

    @Override
    public PROXY_OPTION option() {
        return PROXY_OPTION.PING;
    }

    @Override
    public ProxyOptionParser read(String option) {
        return super.read(option);
    }

    @Override
    public String getPayload() {
        return PING;
    }
}
