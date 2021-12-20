package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.RequestResponseCommand;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.meta.server.dcchange.ChangePrimaryDcAction;

import java.util.concurrent.atomic.AtomicBoolean;

public class ChangePrimaryDcJob extends AbstractCommand<MetaServerConsoleService.PrimaryDcChangeMessage>
        implements RequestResponseCommand<MetaServerConsoleService.PrimaryDcChangeMessage> {

    private ChangePrimaryDcAction action;

    private Long cluster;

    private Long shard;

    private String newPrimaryDc;

    private MasterInfo masterInfo;

    protected AtomicBoolean started = new AtomicBoolean(false);

    public ChangePrimaryDcJob(ChangePrimaryDcAction action, Long cluster, Long shard,
                              String newPrimaryDc, MasterInfo masterInfo) {
        this.action = action;
        this.cluster = cluster;
        this.shard = shard;
        this.newPrimaryDc = newPrimaryDc;
        this.masterInfo = masterInfo;
    }

    @Override
    protected void doExecute() throws Exception {
        started.set(true);
        try {
            MetaServerConsoleService.PrimaryDcChangeMessage result = action
                    .changePrimaryDc(cluster, shard, newPrimaryDc, masterInfo);
            future().setSuccess(result);
        } catch (Exception e) {
            future().setFailure(e);
        }
    }

    @Override
    protected void doReset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public int getCommandTimeoutMilli() {
        return 2000;
    }

    public boolean isStarted() {
        return started.get();
    }
}
