package com.ctrip.xpipe.redis.proxy.session;

/**
 * @author chen.zhu
 * <p>
 * May 30, 2018
 */
public interface SessionEventHandler {

    void onInit();

    void onEstablished();

    void onWritable();

    void onNotWritable();

}
