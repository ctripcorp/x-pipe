package com.ctrip.xpipe.redis.core.proxy.parser.route;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyOptionParser;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public interface ProxyRouteParser extends ProxyOptionParser {

    void removeNextNodes();

    List<Endpoint> getNextEndpoints();
}
