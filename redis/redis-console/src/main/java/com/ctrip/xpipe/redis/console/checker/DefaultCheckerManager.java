package com.ctrip.xpipe.redis.console.checker;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.CheckerStatus;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerMode;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;

import static com.ctrip.xpipe.redis.checker.model.CheckerRole.LEADER;

/**
 * @author lishanglin
 * date 2021/3/16
 */
@Component
@ConsoleServerMode(ConsoleServerModeCondition.SERVER_MODE.CONSOLE)
public class DefaultCheckerManager implements CheckerManager {

    private List<Map<HostPort, CheckerStatus>> checkers;

    private ConsoleConfig config;

    private static final Logger logger = LoggerFactory.getLogger(DefaultCheckerManager.class);

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask refreshTask;

    @Autowired
    public DefaultCheckerManager(ConsoleConfig consoleConfig) {
        this.config = consoleConfig;
        this.checkers = new ArrayList<>();
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("CheckerStatusRefresher"));
        this.refreshTask = new DynamicDelayPeriodTask("CheckerStatusRefresher", this::expireCheckers,
                config::getCheckerAckTimeoutMilli, scheduled);
    }

    @PostConstruct
    public void postConstruct() {
        try {
            this.refreshTask.start();
        } catch (Throwable th) {
            logger.info("[postConstruct] start refresh fail", th);
        }
    }

    @Override
    public void refreshCheckerStatus(CheckerStatus checkerStatus) {
        logger.debug("[refreshCheckerStatus][{}] {}:{} {}",
                checkerStatus.getHostPort(), checkerStatus.getPartIndex(), checkerStatus.getCheckerRole(), checkerStatus.getAllCheckerRole());
        checkerStatus.setLastAckTime(System.currentTimeMillis());
        int partIndex = checkerStatus.getPartIndex();
        List<Map<HostPort, CheckerStatus>> localCheckers = checkers;

        if (localCheckers.size() <= partIndex) {
            logger.info("[refreshCheckerStatus] outbound index {}", checkerStatus);
            return;
        }

        logger.info("[refreshCheckerStatus] update index {} status {}", partIndex, checkerStatus);
        localCheckers.get(partIndex).put(checkerStatus.getHostPort(), checkerStatus);
    }

    @Override
    public List<Map<HostPort, CheckerStatus>> getCheckers() {
        return checkers;
    }

    @Override
    public List<ConsoleCheckerService> getLeaderCheckerServices() {
        List<ConsoleCheckerService> services = new ArrayList<>();
        for (Map<HostPort, CheckerStatus> checker : checkers) {
            for (CheckerStatus value : checker.values()) {
                if (LEADER.equals(value.getCheckerRole())) {
                    services.add(new DefaultConsoleCheckerService(value.getHostPort()));
                }
            }
        }
        return services;
    }

    @Override
    public List<HostPort> getClusterCheckerManager(long clusterId) {
        int totalParts = config.getClusterDividedParts();
        int index = (int) (clusterId % totalParts);
        return new ArrayList<>(checkers.get(index).keySet());
    }

    @Override
    public HostPort getClusterCheckerLeader(long clusterId) {
        int totalParts = config.getClusterDividedParts();
        int index = (int) (clusterId % totalParts);
        for (Map.Entry<HostPort, CheckerStatus> entry : checkers.get(index).entrySet()) {
            if (entry.getValue().getCheckerRole().equals(LEADER)) {
                return entry.getKey();
            }
        }
        return null;
    }

    @VisibleForTesting
    protected void expireCheckers() {
        logger.debug("[expireCheckers] start");
        int totalParts = config.getClusterDividedParts();
        if (totalParts != checkers.size()) {
            List<Map<HostPort, CheckerStatus>> newCheckers = new ArrayList<>(totalParts);
            IntStream.range(0, totalParts).forEach(i -> {
                if (i < checkers.size()) newCheckers.add(checkers.get(i));
                else newCheckers.add(new ConcurrentHashMap<>());
            });

            this.checkers = newCheckers;
        }

        this.checkers.forEach(checkerStatuses -> {
            checkerStatuses.values().forEach(checkerStatus -> {
                if (isCheckerStatusExpired(checkerStatus)) {
                    logger.info("[expireCheckers] expire {}", checkerStatus);
                    this.checkers.get(checkerStatus.getPartIndex()).remove(checkerStatus.getHostPort());
                }
            });
        });
    }

    private boolean isCheckerStatusExpired(CheckerStatus checkerStatus) {
        long current = System.currentTimeMillis();
        return current - checkerStatus.getLastAckTime() > config.getCheckerAckTimeoutMilli();
    }

}
