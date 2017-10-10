package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 11, 2017
 */
public abstract class AbstractCreateInfo {

    @JsonIgnore
    protected Logger logger = LoggerFactory.getLogger(getClass());

    public abstract void check() throws CheckFailException;
}
