package com.ctrip.xpipe.redis.keeper.applier.client;

import com.ctrip.xpipe.api.command.CommandFuture;

/**
 * @author Slight
 * <p>
 * Jan 29, 2022 4:14 PM
 */
public interface ApplierRedisClient {

    CommandFuture<Boolean> set(byte[] key, byte[] value);

    CommandFuture<Boolean> delete(byte[] key);
}
