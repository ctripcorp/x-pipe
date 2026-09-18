package com.ctrip.xpipe.redis.console.keeper.command;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.service.KeeperContainerCheckerService;

public class KeeperResetCommand<T> extends AbstractCommand<T> {

    private String ip;
    private String hostname;
    private long shardId;

    private KeeperContainerCheckerService keeperContainerService;

    public KeeperResetCommand(String ip, String hostname, long shardId, KeeperContainerCheckerService keeperContainerService) {
        this.ip = ip;
        this.hostname = hostname;
        this.shardId = shardId;
        this.keeperContainerService = keeperContainerService;
    }

    @Override
    public String getName() {
        return "KeeperResetCommand";
    }

    @Override
    protected void doExecute() throws Throwable {
        keeperContainerService.resetKeeper(ip, hostname, shardId);
        this.future().setSuccess();
    }

    @Override
    protected void doReset() {

    }
}
