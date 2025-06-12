package com.ctrip.xpipe.redis.console.checker.command;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.checker.ConsoleCheckerApiService;

public class InstanceHealthCheckGetCommand extends AbstractCommand<String> {

    ConsoleCheckerApiService service;

    HostPort checker;

    String  ip;

    int port;

    boolean isCrossRegion;

    public InstanceHealthCheckGetCommand(ConsoleCheckerApiService service, HostPort checker, String ip, int port, boolean isCrossRegion) {
        this.service = service;
        this.checker = checker;
        this.ip = ip;
        this.port = port;
        this.isCrossRegion = isCrossRegion;
    }

    @Override
    public String getName() {
        return "InstanceHealthCheckGetCommand";
    }

    @Override
    protected void doExecute() throws Throwable {
        if (isCrossRegion) {
            future().setSuccess(service.getCrossRegionHealthCheckInstance(checker, ip, port));
        } else {
            future().setSuccess(service.getHealthCheckInstance(checker, ip, port));
        }
    }

    @Override
    protected void doReset() {

    }
}
