package com.ctrip.framework.xpipe.redis.proxy;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ctrip.framework.xpipe.redis.utils.Constants.*;

public class RouteOptionParser {

    private String[] nodes;

    private String[] nextNodes;

    protected String originOptionString;

    private AtomicBoolean nextNodesRemoved = new AtomicBoolean(false);

    public List<InetSocketAddress> getNextEndpoints() {
        if (nextNodes == null || nextNodes.length == 0) {
            return null;
        }
        List<InetSocketAddress> result = new ArrayList<>();
        for (String rawUri : nextNodes) {
            try {
                URI uri = new URI(rawUri);
                result.add(new InetSocketAddress(uri.getHost(), uri.getPort()));
            } catch (Exception e) {
            }
        }
        return result;
    }

    public RouteOptionParser read(String option) {
        if (option == null || option.isEmpty() || option.length() <= ROUTE.length() + 1) {
            return this;
        }
        this.originOptionString = option.substring(ROUTE.length() + 1);
        this.nodes = originOptionString.split(ELEMENT_SPLITTER);
        this.nextNodes = nodes[0].split(ARRAY_SPLITTER);
        return this;
    }

    public String output() {
        removeNextNodes();
        StringBuilder sb = new StringBuilder();
        sb.append(ROUTE);
        for (String node : nodes) {
            sb.append(WHITE_SPACE).append(node);
        }
        return sb.toString();
    }

    public void removeNextNodes() {
        if (nextNodesRemoved.compareAndSet(false, true) && nodes.length > 0) {
            String[] newNodes = new String[nodes.length - 1];
            System.arraycopy(nodes, 1, newNodes, 0, newNodes.length);
            nodes = newNodes;
        }
    }

}
