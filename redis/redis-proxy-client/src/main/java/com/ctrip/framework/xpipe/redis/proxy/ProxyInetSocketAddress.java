package com.ctrip.framework.xpipe.redis.proxy;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * @author Slight
 * <p>
 * Sep 06, 2021 9:09 PM
 */
public class ProxyInetSocketAddress extends InetSocketAddress {

    public volatile boolean sick = false;

    public volatile boolean down = false;

    public int reference = 0;

    public ProxyInetSocketAddress(int port) {
        super(port);
    }

    public ProxyInetSocketAddress(InetAddress addr, int port) {
        super(addr, port);
    }

    public ProxyInetSocketAddress(String hostname, int port) {
        super(hostname, port);
    }
}
