package com.ctrip.xpipe.redis.console.checker.command;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.checker.ConsoleCheckerApiService;
import com.ctrip.xpipe.redis.console.checker.ConsoleCheckerGroupService;
import com.ctrip.xpipe.tuple.Pair;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class InstanceHealthCheckGetGroupCommand extends AbstractCommand<Map<HostPort, String>> {

    ConsoleCheckerApiService service;

    List<HostPort> checkers;

    String  ip;

    int port;

    ExecutorService executor;

    public InstanceHealthCheckGetGroupCommand(ConsoleCheckerApiService service, List<HostPort> checkers, String ip, int port, ExecutorService executor) {
        this.service = service;
        this.checkers = checkers;
        this.ip = ip;
        this.port = port;
        this.executor = executor;
    }

    @Override
    public String getName() {
        return "InstanceHealthCheckGetGroupCommand";
    }

    @Override
    protected void doExecute() {
        Map<HostPort, CommandFuture<String>> futureMap = new HashMap<>();
        checkers.forEach(checker -> {
            CommandFuture<String> future = new InstanceHealthCheckGetCommand(service, checker, ip, port).execute(executor);
            futureMap.put(checker, future);
        });
        Map<HostPort, String> result = new HashMap<>();
        for (Map.Entry<HostPort, CommandFuture<String>> entry : futureMap.entrySet() ) {
            try {
                result.put(entry.getKey(), entry.getValue().get());
            } catch (InterruptedException | ExecutionException e) {
                result.put(entry.getKey(), e.getMessage());
            }
        }
        future().setSuccess(result);
    }

    @Override
    protected void doReset() {

    }
}
