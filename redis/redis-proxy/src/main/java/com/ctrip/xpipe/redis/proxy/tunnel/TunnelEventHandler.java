package com.ctrip.xpipe.redis.proxy.tunnel;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public interface TunnelEventHandler {

    void onEstablished();

    void onBackendClose();

    void onFrontendClose();

    void onClosing();

    void onClosed();
}
