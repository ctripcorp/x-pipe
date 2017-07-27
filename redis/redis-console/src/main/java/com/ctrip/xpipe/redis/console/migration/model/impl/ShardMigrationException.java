package com.ctrip.xpipe.redis.console.migration.model.impl;

import com.ctrip.xpipe.exception.XpipeException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 27, 2017
 */
public class ShardMigrationException extends XpipeException{

    public ShardMigrationException(String message){
        super(message);
    }

    public ShardMigrationException(String message, Throwable th) {
        super(message, th);
    }
}
