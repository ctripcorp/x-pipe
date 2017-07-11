package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 11, 2017
 */
public class CheckFailException extends RedisConsoleException{

    public CheckFailException(String message) {
        super(message);
    }
}
