package com.ctrip.xpipe.redis.meta.server.crdt.master;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

public interface MasterChooseCommand extends Command<RedisMeta> {

}
