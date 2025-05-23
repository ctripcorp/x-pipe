package com.ctrip.xpipe.redis.console.keeper.command;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.checker.KeeperContainerCheckerService;

public class KeeperResetCommand<T> extends AbstractCommand<T> {

    private String activeKeeperIp;

    private long shardId;

    private KeeperContainerCheckerService keeperContainerService;

    public KeeperResetCommand(String activeKeeperIp, long shardId, KeeperContainerCheckerService keeperContainerService) {
        this.activeKeeperIp = activeKeeperIp;
        this.shardId = shardId;
        this.keeperContainerService = keeperContainerService;
    }

    @Override
    public String getName() {
        return "KeeperResetCommand";
    }

    @Override
    protected void doExecute() throws Throwable {
        keeperContainerService.resetKeeper(activeKeeperIp, shardId);
        this.future().setSuccess();
    }

    @Override
    protected void doReset() {

    }
}
