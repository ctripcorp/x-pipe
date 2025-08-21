package com.ctrip.xpipe.redis.console.checker.command;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.console.checker.ConsoleCheckerApiService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class InstanceHealthStatusGetGroupCommand extends AbstractCommand<Map<HostPort, HEALTH_STATE>> {

    ConsoleCheckerApiService service;

    List<HostPort> checkers;

    String  ip;

    int port;

    boolean isCrossRegion;

    ExecutorService executor;

    public InstanceHealthStatusGetGroupCommand(ConsoleCheckerApiService service, List<HostPort> checkers, String ip, int port, boolean isCrossRegion, ExecutorService executor) {
        this.service = service;
        this.checkers = checkers;
        this.ip = ip;
        this.port = port;
        this.isCrossRegion = isCrossRegion;
        this.executor = executor;
    }

    @Override
    public String getName() {
        return "InstanceHealthCheckGetGroupCommand";
    }

    @Override
    protected void doExecute() throws Throwable {
        Map<HostPort, CommandFuture<HEALTH_STATE>> futureMap = new HashMap<>();
        checkers.forEach(checker -> {
            CommandFuture<HEALTH_STATE> future = new InstanceHealthStatusGetCommand(service, checker, ip, port, isCrossRegion).execute(executor);
            futureMap.put(checker, future);
        });
        Map<HostPort, HEALTH_STATE> result = new HashMap<>();
        for (Map.Entry<HostPort, CommandFuture<HEALTH_STATE>> entry : futureMap.entrySet() ) {
            try {
                result.put(entry.getKey(), entry.getValue().get());
            } catch (InterruptedException | ExecutionException e) {
                result.put(entry.getKey(), HEALTH_STATE.UNKNOWN);
            }
        }
        future().setSuccess(result);
    }

    @Override
    protected void doReset() {

    }
}
