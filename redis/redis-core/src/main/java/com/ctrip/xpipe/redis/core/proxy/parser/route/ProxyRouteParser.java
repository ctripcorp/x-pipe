package com.ctrip.xpipe.redis.core.proxy.parser.route;

import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyOptionParser;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public interface ProxyRouteParser extends ProxyOptionParser {

    void removeNextNodes();

    String getContent();

    List<ProxyEndpoint> getNextEndpoints();

    String getFinalStation();

    boolean isNearDest();
}
