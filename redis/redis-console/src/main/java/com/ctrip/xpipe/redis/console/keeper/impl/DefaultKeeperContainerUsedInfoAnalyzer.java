package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.Command.AbstractGetAllDcCommand;
import com.ctrip.xpipe.redis.console.keeper.Command.KeeperContainerFullSynchronizationTimeGetCommand;
import com.ctrip.xpipe.redis.console.keeper.Command.KeeperContainerInfoGetCommand;
import com.ctrip.xpipe.redis.console.keeper.Command.MigrationKeeperContainerDetailInfoGetCommand;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
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

    private Map<Integer, Pair<List<KeeperContainerUsedInfoModel>, Date>> keeperContainerUsedInfoModelIndexMap = new HashMap<>();

    private Map<String, KeeperContainerUsedInfoModel> currentDcAllKeeperContainerUsedInfoModelMap = new HashMap<>();

    private List<MigrationKeeperContainerDetailModel> currentDcKeeperContainerMigrationResult = new ArrayList<>();

    private long currentDcMaxKeeperContainerActiveRedisUsedMemory;

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter().toUpperCase();

    public DefaultKeeperContainerUsedInfoAnalyzer() {}

    public DefaultKeeperContainerUsedInfoAnalyzer(ConsoleConfig config,
                                                  KeeperContainerMigrationAnalyzer migrationAnalyzer) {
        this.config = config;
        this.migrationAnalyzer = migrationAnalyzer;
    }

    @Override
    public synchronized void updateKeeperContainerUsedInfo(int index, List<KeeperContainerUsedInfoModel> keeperContainerUsedInfoModels) {
        if (keeperContainerUsedInfoModels != null && !keeperContainerUsedInfoModels.isEmpty()){
            keeperContainerUsedInfoModelIndexMap.put(index, new Pair<>(keeperContainerUsedInfoModels, new Date()));
        }
        removeExpireData();
        logger.info("[analyzeKeeperContainerUsedInfo] current index {}", keeperContainerUsedInfoModelIndexMap.keySet());
        if (keeperContainerUsedInfoModelIndexMap.size() != config.getClusterDividedParts()) return;

        currentDcAllKeeperContainerUsedInfoModelMap.clear();
        keeperContainerUsedInfoModelIndexMap.values().forEach(list -> list.getKey().forEach(infoModel -> currentDcAllKeeperContainerUsedInfoModelMap.put(infoModel.getKeeperIp(),
                infoModel.setUpdateTime(new Date(System.currentTimeMillis() + 8 * 60 * 60 * 1000)))));

        keeperContainerUsedInfoModelIndexMap.clear();
        logger.info("[analyzeKeeperContainerUsedInfo] start analyze allKeeperContainerUsedInfoModelsList");
        if (currentDcAllKeeperContainerUsedInfoModelMap.isEmpty()) return;
        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                TransactionMonitor transaction = TransactionMonitor.DEFAULT;
                transaction.logTransactionSwallowException("keeperContainer.analyze", currentDc, new Task() {
                    @Override
                    public void go() throws Exception {
                        currentDcKeeperContainerMigrationResult = migrationAnalyzer.getMigrationPlans(currentDcAllKeeperContainerUsedInfoModelMap);
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
    public List<Integer> getAllDcMaxKeeperContainerFullSynchronizationTime() {
        return getAllDcResult(this::getCurrentDcMaxKeeperContainerFullSynchronizationTime,
                new KeeperContainerFullSynchronizationTimeGetCommand(restTemplate),
                Collections.synchronizedList(new ArrayList<>()));
    }

    @Override
    public List<MigrationKeeperContainerDetailModel> getCurrentDcReadyToMigrationKeeperContainers() {
        return currentDcKeeperContainerMigrationResult;
    }

    @Override
    public List<KeeperContainerUsedInfoModel> getCurrentDcKeeperContainerUsedInfoModelsList() {
        return new ArrayList<>(currentDcAllKeeperContainerUsedInfoModelMap.values());
    }

    @Override
    public List<Integer> getCurrentDcMaxKeeperContainerFullSynchronizationTime() {
        List<Integer> result = new ArrayList<>();
        double keeperContainerIoRate = config.getKeeperContainerIoRate();
        if (!currentDcAllKeeperContainerUsedInfoModelMap.isEmpty()) {
            currentDcMaxKeeperContainerActiveRedisUsedMemory = getMaxActiveRedisUsedMemory(currentDcAllKeeperContainerUsedInfoModelMap);
        }
        result.add((int) (currentDcMaxKeeperContainerActiveRedisUsedMemory /1024/1024/keeperContainerIoRate/60));
        return result;
    }

    private long getMaxActiveRedisUsedMemory(Map<String, KeeperContainerUsedInfoModel> usedInfo) {
        long max = 0;
        for (KeeperContainerUsedInfoModel usedInfoModel : usedInfo.values()) {
            max = Math.max(max, usedInfoModel.getActiveRedisUsedMemory());
        }
        return max;
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

    private void removeExpireData() {
        List<Integer> expireIndex = new ArrayList<>();
        for (Map.Entry<Integer, Pair<List<KeeperContainerUsedInfoModel>, Date>> entry : keeperContainerUsedInfoModelIndexMap.entrySet()) {
            if (new Date().getTime() - entry.getValue().getValue().getTime() > config.getKeeperCheckerIntervalMilli() * 2L) {
                expireIndex.add(entry.getKey());
            }
        }
        for (int index : expireIndex) {
            logger.debug("[removeExpireData] remove expire index:{} time:{}, expire time:{}", index, keeperContainerUsedInfoModelIndexMap.get(index).getValue(), config.getKeeperCheckerIntervalMilli() * 2L);
            keeperContainerUsedInfoModelIndexMap.remove(index);
        }
    }

    @VisibleForTesting
    int getCheckerIndexesSize() {
        return keeperContainerUsedInfoModelIndexMap.size();
    }

    @VisibleForTesting
    void setExecutors(Executor executors){
        this.executors = executors;
    }

}
