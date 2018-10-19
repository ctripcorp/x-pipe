package com.ctrip.xpipe.redis.core.proxy.parser;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2018
 */
public abstract class AbstractProxyProtocolResponse<T> implements ProxyProtocolResponse<T> {

    protected T payload;

}
