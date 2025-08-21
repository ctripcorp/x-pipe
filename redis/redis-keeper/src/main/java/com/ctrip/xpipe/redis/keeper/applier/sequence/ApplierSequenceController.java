package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpCommand;

/**
 * @author Slight
 * <p>
 * Jan 29, 2022 4:11 PM
 */
public interface ApplierSequenceController extends Lifecycle {

    void submit(RedisOpCommand<?> command, long commandOffsetToAccumulate, GtidSet gtidSet);
}
