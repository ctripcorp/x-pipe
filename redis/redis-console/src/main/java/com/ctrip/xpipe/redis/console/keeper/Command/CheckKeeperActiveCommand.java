package com.ctrip.xpipe.redis.console.keeper.Command;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.utils.VisibleForTesting;

import java.util.concurrent.ScheduledExecutorService;

public class CheckKeeperActiveCommand<T> extends AbstractKeeperCommand<T>{

    private Endpoint keeper;

    private boolean expectedActive;

    public CheckKeeperActiveCommand(XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled, Endpoint keeper, boolean expectedActive) {
        super(keyedObjectPool, scheduled);
        this.keeper = keeper;
        this.expectedActive = expectedActive;
    }

    @Override
    public String getName() {
        return "CheckKeeperActiveCommand";
    }

    @Override
    protected void doExecute() throws Throwable {
        InfoCommand infoCommand = generateInfoReplicationCommand(keeper);
        if (new InfoResultExtractor(infoCommand.execute().get()).isKeeperActive() == expectedActive) {
            this.future().setSuccess();
            return;
        }
        this.future().setFailure(new Exception(String.format("keeper: %s is not %s", keeper, expectedActive)));
    }

    @Override
    protected void doReset() {

    }
}
