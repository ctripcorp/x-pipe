package com.ctrip.xpipe.redis.meta.server.crdt.peermaster;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

public interface PeerMasterChooseCommand extends Command<RedisMeta> {

    RedisMeta choose() throws Exception;

}
