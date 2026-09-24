package com.ctrip.xpipe.redis.console.keeper.command;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.service.KeeperContainerCheckerService;

public class KeeperResetCommand<T> extends AbstractCommand<T> {

    private String ip;
    private long shardId;

    private KeeperContainerCheckerService keeperContainerService;

    public KeeperResetCommand(String ip, long shardId, KeeperContainerCheckerService keeperContainerService) {
        this.ip = ip;
        this.shardId = shardId;
        this.keeperContainerService = keeperContainerService;
    }

    @Override
    public String getName() {
        return "KeeperResetCommand";
    }

    @Override
    protected void doExecute() throws Throwable {
        // 该命令按 IP 直连，hostname 为空不会触发反查，因此无需传 dc
        keeperContainerService.resetKeeper(null, ip, null, shardId);
        this.future().setSuccess();
    }

    @Override
    protected void doReset() {

    }
}
