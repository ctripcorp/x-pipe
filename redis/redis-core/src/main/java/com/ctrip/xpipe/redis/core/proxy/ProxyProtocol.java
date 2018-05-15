package com.ctrip.xpipe.redis.core.proxy;

import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.parser.compress.CompressAlgorithm;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface ProxyProtocol {

    List<ProxyEndpoint> nextEndpoints();

    void recordPath(Channel channel);

    ByteBuf output();

    ProxyProtocol read(ByteBuf byteBuf);

    void setContent(String content);

    String getContent();

    boolean isCompressed();

    CompressAlgorithm compressAlgorithm();

}
