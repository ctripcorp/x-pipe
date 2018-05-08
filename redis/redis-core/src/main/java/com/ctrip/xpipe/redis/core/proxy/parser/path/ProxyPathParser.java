package com.ctrip.xpipe.redis.core.proxy.parser.path;

import com.ctrip.xpipe.redis.core.proxy.parser.ProxyOptionParser;
import io.netty.channel.Channel;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public interface ProxyPathParser extends ProxyOptionParser {

    void addNodeToPath(Channel channel);
}
