package com.ctrip.xpipe.redis.core.proxy.protocols;

import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocolParser;
import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public abstract class AbstractProxyProtocol<T extends ProxyProtocolParser> implements ProxyProtocol {

    protected T parser;

    protected AbstractProxyProtocol(T parser) {
        this.parser = parser;
    }

    @Override
    public ByteBuf output() {
        return parser.format();
    }

    public T getParser() {
        return parser;
    }
}
