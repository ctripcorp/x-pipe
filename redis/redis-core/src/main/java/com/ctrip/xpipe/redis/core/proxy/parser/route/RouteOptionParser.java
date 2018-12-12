package com.ctrip.xpipe.redis.core.proxy.parser.route;

import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public class RouteOptionParser extends AbstractProxyOptionParser implements ProxyRouteParser {

    private String[] nodes;

    private String[] nextNodes;

    private AtomicBoolean nextNodesRemoved = new AtomicBoolean(false);

    @Override
    public PROXY_OPTION option() {
        return PROXY_OPTION.ROUTE;
    }

    @Override
    public void removeNextNodes() {
        if(nextNodesRemoved.compareAndSet(false, true)) {
            if(nodes.length < 1)    return;
            String[] newNodes = new String[nodes.length - 1];
            System.arraycopy(nodes, 1, newNodes, 0, newNodes.length);
            nodes = newNodes;
        }
    }

    @Override
    public String getContent() {
        return originOptionString;
    }

    @Override
    public List<ProxyEndpoint> getNextEndpoints() {
        if(nextNodes == null || nextNodes.length == 0) {
            return null;
        }
        List<ProxyEndpoint> result = Lists.newArrayList();
        for(String rawUri : nextNodes) {
            result.add(new DefaultProxyEndpoint(rawUri));
        }
        return result;
    }

    @Override
    public String getFinalStation() {
        if(nodes != null && nodes.length > 0) {
            return nodes[nodes.length - 1];
        } else {
            return "last-stop";
        }
    }

    @Override
    public RouteOptionParser read(String option) {
        if(option == null || option.isEmpty() || option.length() <= option().name().length() + 1) {
            return this;
        }
        this.originOptionString = option.substring(option().name().length() + 1);
        this.nodes = originOptionString.split(ELEMENT_SPLITTER);
        this.nextNodes = nodes[0].split(ARRAY_SPLITTER);
        return this;
    }

    @Override
    public String output() {
        removeNextNodes();
        return option().name() + " " + StringUtil.join(WHITE_SPACE, nodes);
    }
//
//    @Override
//    public boolean isImportant() {
//        return true;
//    }
}
