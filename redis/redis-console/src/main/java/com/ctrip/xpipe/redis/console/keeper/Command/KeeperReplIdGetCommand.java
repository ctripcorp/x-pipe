package com.ctrip.xpipe.redis.console.keeper.command;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.service.KeeperSessionService;


public class KeeperReplIdGetCommand extends AbstractCommand<Long> {

    KeeperSessionService service;

    HostPort keeper;

    public KeeperReplIdGetCommand(KeeperSessionService service, HostPort keeper) {
        this.service = service;
        this.keeper = keeper;
    }

    @Override
    public String getName() {
        return "KeeperReplIdGetCommand";
    }

    @Override
    protected void doExecute() throws Throwable {
        future().setSuccess(service.getKeeperReplId(keeper.getHost(), keeper.getPort()).getReplId());
    }

    @Override
    protected void doReset() {

    }
}
