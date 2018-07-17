package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.utils.IpUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Jul 16, 2018
 */
public class InternalNetworkHandler extends ChannelInboundHandlerAdapter {

    private Logger logger = LoggerFactory.getLogger(InternalNetworkHandler.class);

    private String[] internalIpPrefix;

    public InternalNetworkHandler(String... internalIpPrefix) {
        this.internalIpPrefix = internalIpPrefix;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        if(!isInternalNetwork(channel)) {
            logger.error("[not internal network] {}, close channel", channel.remoteAddress());
            channel.close();
        }
        super.channelActive(ctx);
    }

    private boolean isInternalNetwork(Channel channel) {
        String remoteHost = IpUtils.getIp(channel.remoteAddress());
        String[] splittedBytes = IpUtils.splitIpAddr(remoteHost);
        for(int i = 0; internalIpPrefix != null && i < internalIpPrefix.length; i++) {
            if(!splittedBytes[i].equals(internalIpPrefix[i])) {
                return false;
            }
        }
        return true;
    }
}
