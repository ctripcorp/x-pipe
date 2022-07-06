package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.redis.keeper.handler.applier.ApplierCommandHandler;
import com.ctrip.xpipe.redis.keeper.handler.applier.ApplierInfoHandler;
import com.ctrip.xpipe.redis.keeper.handler.applier.ApplierRoleCommandHandler;

/**
 * @author lishanglin
 * date 2022/6/24
 */
public class ApplierCommandHandlerManager extends CommandHandlerManager {

    protected void initCommands() {
        putHandler(new ApplierCommandHandler());
        putHandler(new ApplierRoleCommandHandler());
        putHandler(new ApplierInfoHandler());

        putHandler(new PingCommandHandler());
        putHandler(new LFHandler());
        putHandler(new ClientCommandHandler());
    }

}
