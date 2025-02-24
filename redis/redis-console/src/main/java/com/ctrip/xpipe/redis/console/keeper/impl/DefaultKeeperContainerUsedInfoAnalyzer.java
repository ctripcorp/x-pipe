package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.RedisMsg;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.command.AbstractGetAllDcCommand;
import com.ctrip.xpipe.redis.console.keeper.command.KeeperContainerInfoGetCommand;
import com.ctrip.xpipe.redis.console.keeper.command.MigrationKeeperContainerDetailInfoGetCommand;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


public class DefaultKeeperContainerUsedInfoAnalyzer extends AbstractService implements KeeperContainerUsedInfoAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeeperContainerUsedInfoAnalyzer.class);

    private ConsoleConfig config;

    private KeeperContainerMigrationAnalyzer migrationAnalyzer;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    private Executor executors;

    private Map<String, KeeperContainerUsedInfoModel> currentDcAllKeeperContainerUsedInfoModelMap = new HashMap<>();

    private List<MigrationKeeperContainerDetailModel> keeperContainerMigrationResult = new ArrayList<>();

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter().toUpperCase();

    public DefaultKeeperContainerUsedInfoAnalyzer() {}

    public DefaultKeeperContainerUsedInfoAnalyzer(ConsoleConfig config,
                                                  KeeperContainerMigrationAnalyzer migrationAnalyzer) {
        this.config = config;
        this.migrationAnalyzer = migrationAnalyzer;
    }

    @Override
    public void updateKeeperContainerUsedInfo(Map<String, KeeperContainerUsedInfoModel> modelMap) {
        modelMap.values().forEach(model -> model.setUpdateTime(DateTimeUtils.currentTimeAsString()));
        currentDcAllKeeperContainerUsedInfoModelMap = modelMap;
        if (currentDcAllKeeperContainerUsedInfoModelMap.isEmpty()) return;
        logger.info("[analyzeKeeperContainerUsedInfo] start analyze allKeeperContainerUsedInfoModelsList");
        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                TransactionMonitor transaction = TransactionMonitor.DEFAULT;
                transaction.logTransactionSwallowException("keeperContainer.analyze", currentDc, new Task() {
                    @Override
                    public void go() throws Exception {
                        keeperContainerMigrationResult = migrationAnalyzer.getMigrationPlans(currentDcAllKeeperContainerUsedInfoModelMap);
                    }

                    @Override
                    public Map<String, Object> getData() {
                        Map<String, Object> transactionData = new HashMap<>();
                        transactionData.put("keeperContainerSize", currentDcAllKeeperContainerUsedInfoModelMap.size());
                        return transactionData;
                    }
                });
            }
        });
    }

    @Override
    public List<MigrationKeeperContainerDetailModel> getAllDcReadyToMigrationKeeperContainers() {
        return getAllDcResult(this::getCurrentDcReadyToMigrationKeeperContainers,
                new MigrationKeeperContainerDetailInfoGetCommand(restTemplate),
                Collections.synchronizedList(new ArrayList<>()));
    }

    @Override
    public List<KeeperContainerUsedInfoModel> getAllDcKeeperContainerUsedInfoModelsList() {
        return getAllDcResult(this::getCurrentDcKeeperContainerUsedInfoModelsList,
                new KeeperContainerInfoGetCommand(restTemplate),
                Collections.synchronizedList(new ArrayList<>()));
    }

    @Override
    public List<MigrationKeeperContainerDetailModel> getCurrentDcReadyToMigrationKeeperContainers() {
        return keeperContainerMigrationResult;
    }

    @Override
    public List<KeeperContainerUsedInfoModel> getCurrentDcKeeperContainerUsedInfoModelsList() {
        return new ArrayList<>(currentDcAllKeeperContainerUsedInfoModelMap.values());
    }

    public <T> List<T> getAllDcResult(Supplier<List<T>> localDcResultSupplier, AbstractGetAllDcCommand<List<T>> command, List<T> result) {
        logger.info("[getAllDcResult] {} start", command.getName());
        ParallelCommandChain commandChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
        config.getConsoleDomains().forEach((dc, domains) -> {
            if (currentDc.equalsIgnoreCase(dc)) {
                result.addAll(localDcResultSupplier.get());
            } else {
                AbstractGetAllDcCommand<List<T>> newCommand = command.clone();
                newCommand.setDomain(domains);
                newCommand.future().addListener(commandFuture -> {
                    if (commandFuture.isSuccess() && commandFuture.get() != null) {
                        logger.info("[getAllDcResult] getDc:{} Result success", dc);
                        result.addAll(commandFuture.get());
                    }
                });
                commandChain.add(newCommand);
            }
        });

        try {
            commandChain.execute().get(10, TimeUnit.SECONDS);
        } catch (Throwable th) {
            logger.warn("[getAllDcResult][{}] error:", command.getName(), th);
        }

        return result;
    }

    @VisibleForTesting
    void setExecutors(Executor executors){
        this.executors = executors;
    }

}
