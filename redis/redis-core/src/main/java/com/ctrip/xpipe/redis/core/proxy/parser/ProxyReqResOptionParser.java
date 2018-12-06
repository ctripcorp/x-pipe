package com.ctrip.xpipe.redis.core.proxy.parser;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2018
 */
public interface ProxyReqResOptionParser<T> extends ProxyOptionParser {

    T getContent();
}
