package com.ctrip.xpipe.redis.console.keeper.command;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;

import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.core.protocal.MASTER_STATE.REDIS_REPL_CONNECTED;

public class CheckKeeperConnectedCommand<T> extends AbstractKeeperCommand<T> {

    private Endpoint keeper;

    public CheckKeeperConnectedCommand(XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled, Endpoint keeper) {
        super(keyedObjectPool, scheduled);
        this.keeper = keeper;
    }

    @Override
    public String getName() {
        return "CheckKeeperConnectedCommand";
    }

    @Override
    protected void doExecute() throws Throwable {
        SlaveRole role = (SlaveRole)new RoleCommand(keyedObjectPool.getKeyPool(keeper), scheduled).execute().get();
        if (REDIS_REPL_CONNECTED == role.getMasterState()) {
            this.future().setSuccess();
            return;
        }
        this.future().setFailure(new Exception(String.format("ping %s has no pong response", keeper)));
    }

    @Override
    protected void doReset() {

    }
}
