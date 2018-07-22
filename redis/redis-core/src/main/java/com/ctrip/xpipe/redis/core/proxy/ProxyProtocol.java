package com.ctrip.xpipe.redis.core.proxy;

import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.parser.compress.CompressAlgorithm;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface ProxyProtocol {

    public static final String KEY_WORD = "PROXY";

    List<ProxyEndpoint> nextEndpoints();

    void recordForwardFor(InetSocketAddress address);

    String getForwardFor();

    ByteBuf output();

    void setContent(String content);

    String getContent();

    String getRouteInfo();

    String getFinalStation();

    boolean isCompressed();

    CompressAlgorithm compressAlgorithm();

}
