package com.ctrip.xpipe.redis.console.checker.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.console.checker.CheckerManager;
import com.ctrip.xpipe.redis.console.checker.ConsoleCheckerApiService;
import com.ctrip.xpipe.redis.console.checker.ConsoleCheckerGroupService;
import com.ctrip.xpipe.redis.console.checker.command.InstanceHealthCheckGetGroupCommand;
import com.ctrip.xpipe.redis.console.checker.command.InstanceHealthStatusGetGroupCommand;
import jakarta.annotation.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

@Component
public class DefaultConsoleCheckerGroupService implements ConsoleCheckerGroupService {

    @Autowired(required = false)
    private CheckerManager checkerManager;

    @Autowired
    private ConsoleCheckerApiService checkerApiService;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executor;

    @Override
    public HostPort getCheckerLeader(long clusterDbId) {
        return checkerManager.getClusterCheckerLeader(clusterDbId);
    }

    public List<HostPort> getAllChecker(long clusterDbId) {
        return checkerManager.getClusterCheckerManager(clusterDbId);
    }

    @Override
    public CommandFuture<Map<HostPort, String>> getAllHealthCheckInstance(long clusterDbId, String ip, int port, boolean isCrossRegion) {
        return new InstanceHealthCheckGetGroupCommand(checkerApiService, getAllChecker(clusterDbId), ip, port, isCrossRegion, executor).execute(executor);
    }

    @Override
    public CommandFuture<Map<HostPort, HEALTH_STATE>> getAllHealthStates(long clusterDbId, String ip, int port, boolean isCrossRegion) {
        return new InstanceHealthStatusGetGroupCommand(checkerApiService, getAllChecker(clusterDbId), ip, port, isCrossRegion, executor).execute(executor);
    }

}
