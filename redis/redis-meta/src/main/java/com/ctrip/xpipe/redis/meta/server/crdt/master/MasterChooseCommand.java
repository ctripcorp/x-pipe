package com.ctrip.xpipe.redis.meta.server.crdt.master;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.ProxyRedisMeta;

public interface MasterChooseCommand extends Command<RedisMeta> {

}
