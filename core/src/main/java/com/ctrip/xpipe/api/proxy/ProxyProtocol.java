package com.ctrip.xpipe.api.proxy;

import com.ctrip.xpipe.proxy.ProxyEndpoint;
import io.netty.buffer.ByteBuf;

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
