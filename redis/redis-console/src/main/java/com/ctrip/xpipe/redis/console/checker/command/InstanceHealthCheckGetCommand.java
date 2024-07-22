package com.ctrip.xpipe.redis.console.checker.command;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.checker.ConsoleCheckerApiService;
import com.ctrip.xpipe.redis.console.checker.ConsoleCheckerGroupService;

public class InstanceHealthCheckGetCommand extends AbstractCommand<String> {

    ConsoleCheckerApiService service;

    HostPort checker;

    String  ip;

    int port;

    public InstanceHealthCheckGetCommand(ConsoleCheckerApiService service, HostPort checker, String ip, int port) {
        this.service = service;
        this.checker = checker;
        this.ip = ip;
        this.port = port;
    }

    @Override
    public String getName() {
        return "InstanceHealthCheckGetCommand";
    }

    @Override
    protected void doExecute() throws Throwable {
        future().setSuccess(service.getHealthCheckInstance(checker, ip, port));
    }

    @Override
    protected void doReset() {

    }
}
