package com.ctrip.xpipe.redis.console.keeper.Command;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeeperResetCommand<T> extends AbstractCommand<T> {

    private String activeKeeperIp;

    private long shardId;

    private KeeperContainerService keeperContainerService;

    public KeeperResetCommand(String activeKeeperIp, long shardId, KeeperContainerService keeperContainerService) {
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
