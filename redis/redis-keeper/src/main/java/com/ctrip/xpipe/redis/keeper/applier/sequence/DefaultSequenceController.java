package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.redis.keeper.applier.client.ApplierRedisClient;
import com.ctrip.xpipe.redis.keeper.applier.client.DoNothingRedisClient;
import com.ctrip.xpipe.redis.keeper.applier.command.ApplierRedisCommand;

/**
 * @author Slight
 * <p>
 * Jan 29, 2022 4:19 PM
 */
public class DefaultSequenceController implements ApplierSequenceController {

    private final ApplierRedisClient client;

    public DefaultSequenceController() {
        this.client = new DoNothingRedisClient();
    }

    @Override
    public void submit(ApplierRedisCommand<?> command) {
        //do something with key, decide when to apply
        command.key();
        command.apply(client);
    }
}
