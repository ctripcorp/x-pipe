package com.ctrip.xpipe.redis.console.keeper.command;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;

import java.util.concurrent.ScheduledExecutorService;

public class FullSyncJudgeCommand<T> extends AbstractKeeperCommand<T> {

    private Endpoint activeInstance;

    private Endpoint backUpInstance;

    private long activeMasterReplOffset;

    public FullSyncJudgeCommand(XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled, Endpoint activeInstance, Endpoint backUpInstance, long activeMasterReplOffset) {
        super(keyedObjectPool, scheduled);
        this.activeInstance = activeInstance;
        this.backUpInstance = backUpInstance;
        this.activeMasterReplOffset = activeMasterReplOffset;
    }

    @Override
    public String getName() {
        return "FullSyncJudgeCommand";
    }

    @Override
    protected void doExecute() throws Throwable {
        long backupMasterReplOffset;
        backupMasterReplOffset = new InfoResultExtractor(generateInfoReplicationCommand(backUpInstance).execute().get()).getMasterReplOffset();
        if (backupMasterReplOffset > 0 && activeMasterReplOffset > 0 && backupMasterReplOffset > activeMasterReplOffset) {
            this.future().setSuccess();
            return;
        }
        this.future().setFailure(new Exception(String.format("activeInstance: %s and backUpInstance %s is not full sync", activeInstance, backUpInstance)));
    }

    @Override
    protected void doReset() {

    }



}
