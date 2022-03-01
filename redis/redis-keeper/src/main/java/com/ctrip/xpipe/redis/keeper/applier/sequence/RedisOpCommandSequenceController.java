package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpCommand;

/**
 * @author Slight
 * <p>
 * Mar 01, 2022 9:07 AM
 */
public interface RedisOpCommandSequenceController {

    void submit(RedisOpCommand<?> command);
}
