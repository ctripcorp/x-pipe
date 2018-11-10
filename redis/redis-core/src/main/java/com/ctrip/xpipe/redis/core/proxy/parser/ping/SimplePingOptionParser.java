package com.ctrip.xpipe.redis.core.proxy.parser.ping;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.command.entity.ProxyPongEntity;
import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyOptionParser;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyReqResOptionParser;
import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.channel.Channel;


/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2018
 */
public class SimplePingOptionParser extends AbstractProxyOptionParser implements ProxyReqResOptionParser<ProxyPongEntity> {

    private static final String PING = PROXY_OPTION.PING.name();


    @Override
    public ProxyPongEntity getResponse() {
//        return new ProxyPongEntity(HostPort.fromString(ChannelUtil.getSimpleIpport(channel.localAddress())));
        return null;
    }

    @Override
    public PROXY_OPTION option() {
        return PROXY_OPTION.PING;
    }

    @Override
    public String output() {
        return PING;
    }

    @Override
    public ProxyOptionParser read(String option) {
        return super.read(option);
    }

}
