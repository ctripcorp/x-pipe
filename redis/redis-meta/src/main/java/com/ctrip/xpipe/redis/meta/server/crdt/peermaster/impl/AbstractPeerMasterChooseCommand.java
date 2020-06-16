package com.ctrip.xpipe.redis.meta.server.crdt.peermaster.impl;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.crdt.peermaster.PeerMasterChooseCommand;

public abstract class AbstractPeerMasterChooseCommand extends AbstractCommand<RedisMeta> implements PeerMasterChooseCommand {

    protected String dcId;

    protected String clusterId;

    protected String shardId;

    public AbstractPeerMasterChooseCommand(String dcId, String clusterId, String shardId) {
        this.dcId = dcId;
        this.clusterId = clusterId;
        this.shardId = shardId;
    }

    @Override
    protected void doExecute() throws Exception {
        future().setSuccess(choose());
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    protected void doReset() {
        throw new UnsupportedOperationException();
    }

}
