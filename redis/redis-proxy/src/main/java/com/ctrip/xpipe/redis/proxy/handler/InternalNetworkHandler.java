package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.utils.IpUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.net.InetSocketAddress;

/**
 * @author chen.zhu
 * <p>
 * Jul 16, 2018
 */
public class InternalNetworkHandler extends ChannelInboundHandlerAdapter {

    private static final String INTERNAL_IP_PREFIX = "10";

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        if(!isInternalNetwork(channel)) {
            channel.close();
        }
        super.channelActive(ctx);
    }

    private boolean isInternalNetwork(Channel channel) {
        String remoteHost = IpUtils.getIp(channel.remoteAddress());
        String firstByte = IpUtils.splitIpAddr(remoteHost)[0];
        return firstByte.equals(INTERNAL_IP_PREFIX);
    }
}
