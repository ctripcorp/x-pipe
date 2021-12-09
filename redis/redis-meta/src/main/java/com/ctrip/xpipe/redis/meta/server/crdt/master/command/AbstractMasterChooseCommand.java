package com.ctrip.xpipe.redis.meta.server.crdt.master.command;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooseCommand;

public abstract class AbstractMasterChooseCommand extends AbstractCommand<RedisMeta> implements MasterChooseCommand {

    protected Long clusterDbId, shardDbId;

    public AbstractMasterChooseCommand(Long clusterDbId, Long shardDbId) {
        this.clusterDbId = clusterDbId;
        this.shardDbId = shardDbId;
    }

    @Override
    protected void doExecute() throws Exception {
        future().setSuccess(choose());
    }

    protected abstract RedisMeta choose() throws Exception;

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    protected void doReset() {
        throw new UnsupportedOperationException();
    }

}
