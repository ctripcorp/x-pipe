package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 08, 2017
 */
public class ToIdcNotFoundException extends RedisConsoleException{

    public ToIdcNotFoundException(String msg) {
        super(msg);
    }
}
