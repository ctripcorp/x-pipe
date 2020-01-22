package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.meta.server.dcchange.ChangePrimaryDcAction;

public class ChangePrimaryDcJob extends AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage> {

    private ChangePrimaryDcAction action;

    private String cluster;

    private String shard;

    private String newPrimaryDc;

    private MasterInfo masterInfo;

    public ChangePrimaryDcJob(ChangePrimaryDcAction action, String cluster, String shard,
                              String newPrimaryDc, MasterInfo masterInfo) {
        this.action = action;
        this.cluster = cluster;
        this.shard = shard;
        this.newPrimaryDc = newPrimaryDc;
        this.masterInfo = masterInfo;
    }

    @Override
    protected void doExecute() throws Exception {
        future().setSuccess(action.changePrimaryDc(cluster, shard, newPrimaryDc, masterInfo));
    }

    @Override
    protected void doReset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

}
