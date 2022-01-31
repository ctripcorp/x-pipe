package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.keeper.applier.client.ApplierRedisClient;

/**
 * @author Slight
 *
 * Jan 29, 2022 4:08 PM
 */
public interface ApplierRedisCommand<V> extends Command<V> {

    byte[] key();

    CommandFuture<V> apply(ApplierRedisClient client);
}
