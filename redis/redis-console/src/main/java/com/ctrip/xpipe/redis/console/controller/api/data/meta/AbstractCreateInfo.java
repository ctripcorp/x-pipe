package com.ctrip.xpipe.redis.console.controller.api.data.meta;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 11, 2017
 */
public abstract class AbstractCreateInfo {

    public abstract void check() throws CheckFailException;
}
