package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.keeper.applier.AbstractInstanceComponent;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import com.ctrip.xpipe.redis.keeper.applier.command.*;
import com.ctrip.xpipe.redis.keeper.applier.lwm.ApplierLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.threshold.ConcurrencyThreshold;
import com.ctrip.xpipe.redis.keeper.applier.threshold.MemoryThreshold;
import com.ctrip.xpipe.redis.keeper.applier.threshold.QPSThreshold;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * @author Slight
 * <p>
 * Jan 29, 2022 4:19 PM
 */
public class DefaultSequenceController extends AbstractInstanceComponent implements ApplierSequenceController {

    @InstanceDependency
    public ApplierLwmManager lwmManager;

    @InstanceDependency
    public ExecutorService stateThread;

    @InstanceDependency
    public ScheduledExecutorService scheduled;

    @InstanceDependency
    public AsyncRedisClient client;

    public MemoryThreshold memoryThreshold = new MemoryThreshold(32 * 1024 * 1024/* 32M */);

    public ConcurrencyThreshold concurrencyThreshold = new ConcurrencyThreshold(10000);

    public QPSThreshold qpsThreshold;

    Map<RedisKey, SequenceCommand<?>> runningCommands = new HashMap<>();

    SequenceCommand<?> obstacle;

    ExecutorService workerThreads;

    @Override
    protected void doInitialize() throws Exception {
        workerThreads = Executors.newFixedThreadPool(8);
        qpsThreshold = new QPSThreshold(5000, scheduled);
    }

    @Override
    protected void doDispose() throws Exception {
        workerThreads.shutdown();
    }

    @Override
    public void submit(RedisOpCommand<?> command) {

        memoryThreshold.tryPass(command.redisOp().estimatedSize());
        concurrencyThreshold.tryPass();

        if (qpsThreshold != null) {
            qpsThreshold.tryPass();
        }

        stateThread.execute(()->{
            if (logger.isDebugEnabled()) {
                logger.debug("[submit] commandName={} args={}", command.getName(), Arrays.stream(command.redisOp().buildRawOpArgs()).map(String::new).toArray(String[]::new));
            }
            switch (command.type()) {
                case SINGLE_KEY:
                    submitSingleKeyCommand((RedisOpDataCommand<?>) command);
                    break;
                case MULTI_KEY:
                    submitMultiKeyCommand((RedisOpDataCommand<?>) command);
                    break;
                case OTHER:
                    submitObstacle(command);
                    break;
            }
        });
    }

    private void submitSingleKeyCommand(RedisOpDataCommand<?> command) {

        /* find dependencies */

        List<SequenceCommand<?>> dependencies = new ArrayList<>();

        RedisKey key = command.key();
        SequenceCommand<?> lastSameKey = runningCommands.get(key);
        if (lastSameKey != null) {
            dependencies.add(lastSameKey);
        }

        if (obstacle != null) {
            dependencies.add(obstacle);
        }

        /* make command */

        SequenceCommand<?> current = new SequenceCommand<>(dependencies, new StubbornCommand<>(command, workerThreads), stateThread, workerThreads);

        /* make self a dependency */

        runningCommands.put(key, current);
        forgetWhenSuccess(current, key);

        /* do some stuff when finish */

        mergeGtidWhenSuccess(current, command.gtid());
        releaseMemoryThresholdWhenSuccess(current, command.redisOp().estimatedSize());

        /* run self */

        current.execute();
    }

    private void submitMultiKeyCommand(RedisOpDataCommand<?> command) {

        List<RedisKey> keys = command.keys();
        List<SequenceCommand<?>> dependencies = keys.stream().map(runningCommands::get).filter(Objects::nonNull).collect(Collectors.toList());
        if (obstacle != null) {
            dependencies.add(obstacle);
        }

        MultiDataCommand multiDataCommand = new MultiDataCommand(client, command.redisOpAsMulti(), workerThreads);

        SequenceCommand<?> current = new SequenceCommand<>(dependencies, new StubbornCommand<>(multiDataCommand, workerThreads), stateThread, workerThreads);

        for (RedisKey key : keys) {
            runningCommands.put(key, current);
            forgetWhenSuccess(current, key);
        }

        mergeGtidWhenSuccess(current, command.gtid());
        releaseMemoryThresholdWhenSuccess(current, command.redisOp().estimatedSize());

        current.execute();
    }

    private void submitObstacle(RedisOpCommand<?> command) {

        /* find dependencies */

        List<SequenceCommand<?>> dependencies = new ArrayList<>(runningCommands.values());
        if (obstacle != null) {
            dependencies.add(obstacle);
        }

        /* make command */

        SequenceCommand<?> current = new SequenceCommand<>(dependencies, new StubbornCommand<>(command, workerThreads), stateThread, workerThreads);

        /* make self a dependency */

        runningCommands = new HashMap<>();
        obstacle = current;

        forgetObstacleWhenSuccess(current);

        /* do some stuff when finish */

        mergeGtidWhenSuccess(current, command.gtid());
        releaseMemoryThresholdWhenSuccess(current, command.redisOp().estimatedSize());

        /* run self */

        current.execute();
    }

    private void forgetObstacleWhenSuccess(SequenceCommand<?> sequenceCommand) {
        sequenceCommand.future().addListener((f)->{
            if (f.isSuccess()) {
                if (sequenceCommand == obstacle) {
                    obstacle = null;
                }
            }
        });
    }

    private void forgetWhenSuccess(SequenceCommand<?> sequenceCommand, RedisKey key) {
        sequenceCommand.future().addListener((f)->{
            if (f.isSuccess()) {
                if (sequenceCommand == runningCommands.get(key)) {
                    runningCommands.remove(key);
                }
            }
        });
    }

    private void mergeGtidWhenSuccess(SequenceCommand<?> sequenceCommand, String gtid) {
        sequenceCommand.future().addListener((f)->{
            if (f.isSuccess()) {
                if (gtid != null) {
                    if (lwmManager != null) {
                        lwmManager.submit(gtid);
                    }
                }
            }
        });
    }

    private void releaseMemoryThresholdWhenSuccess(SequenceCommand<?> sequenceCommand, long memory) {
        sequenceCommand.future().addListener((f)->{
            if (f.isSuccess()) {
                concurrencyThreshold.release();
                memoryThreshold.release(memory);
            }
        });
    }
}
