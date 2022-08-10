package com.ctrip.xpipe.api.proxy;

import com.ctrip.xpipe.proxy.ProxyEndpoint;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface ProxyConnectProtocol extends ProxyProtocol {

    List<ProxyEndpoint> nextEndpoints();

    void recordForwardFor(InetSocketAddress address);

    String getForwardFor();

    void setContent(String content);

    String getContent();

    String getRouteInfo();

    String getFinalStation();

    String getSource();

    boolean isCompressed();

    CompressAlgorithm getCompressAlgorithm();

    void removeCompressOptionIfExist();

    void addCompression(CompressAlgorithm algorithm);

    boolean isNearDest();
}
