package com.ctrip.xpipe.redis.core.proxy;

import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public interface TLSEnabled {

    NettySslHandlerFactory getNettySslHandlerFactory();

}
