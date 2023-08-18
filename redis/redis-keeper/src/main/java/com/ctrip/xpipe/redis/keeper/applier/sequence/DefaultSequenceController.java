package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.keeper.applier.AbstractInstanceComponent;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.RedisOpDataCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.SequenceCommand;
import com.ctrip.xpipe.redis.keeper.applier.command.StubbornCommand;
import com.ctrip.xpipe.redis.keeper.applier.lwm.ApplierLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.threshold.BytesPerSecondThreshold;
import com.ctrip.xpipe.redis.keeper.applier.threshold.ConcurrencyThreshold;
import com.ctrip.xpipe.redis.keeper.applier.threshold.MemoryThreshold;
import com.ctrip.xpipe.redis.keeper.applier.threshold.QPSThreshold;
import com.ctrip.xpipe.utils.CloseState;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
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
    public ScheduledExecutorService workerThreads;

    @InstanceDependency
    public ScheduledExecutorService scheduled;

    @InstanceDependency
    public AtomicLong offsetRecorder;

    public MemoryThreshold memoryThreshold;

    public ConcurrencyThreshold concurrencyThreshold;

    public QPSThreshold qpsThreshold;

    public BytesPerSecondThreshold bytesPerSecondThreshold;

    Map<RedisKey, SequenceCommand<?>> runningCommands = new HashMap<>();

    SequenceCommand<?> obstacle;

    private long qpsThresholdValue;

    private long bytesPerSecondThresholdValue;

    public static final long DEFAULT_QPS_THRESHOLD = 5000;

    // 100MB
    public static final long DEFAULT_BYTES_PER_SECOND_THRESHOLD = 100 * 1024 * 1024;

    // 32MB
    public static final long DEFAULT_MEMORY_THRESHOLD = 32 * 1024 * 1024;

    public static final long DEFAULT_CONCURRENCY_THRESHOLD = 10000;

    private static final CloseState closeState = new CloseState();

    public DefaultSequenceController() {
        this(DEFAULT_QPS_THRESHOLD, DEFAULT_BYTES_PER_SECOND_THRESHOLD, DEFAULT_MEMORY_THRESHOLD, DEFAULT_CONCURRENCY_THRESHOLD);
    }

    public DefaultSequenceController(Long qpsThreshold, Long bytesPerSecondThreshold, Long memoryThreshold, Long concurrencyThreshold) {
        this.qpsThresholdValue = qpsThreshold == null ? DEFAULT_QPS_THRESHOLD : qpsThreshold;
        this.bytesPerSecondThresholdValue = bytesPerSecondThreshold == null ? DEFAULT_BYTES_PER_SECOND_THRESHOLD : bytesPerSecondThreshold;
        this.memoryThreshold = new MemoryThreshold(memoryThreshold == null ? DEFAULT_MEMORY_THRESHOLD : memoryThreshold);
        this.concurrencyThreshold = new ConcurrencyThreshold(concurrencyThreshold == null ? DEFAULT_CONCURRENCY_THRESHOLD : concurrencyThreshold);
    }

    @Override
    protected void doInitialize() throws Exception {
        qpsThreshold = new QPSThreshold(qpsThresholdValue, scheduled);
        bytesPerSecondThreshold = new BytesPerSecondThreshold(bytesPerSecondThresholdValue, scheduled);
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        closeState.setClosed();
        runningCommands.clear();
        memoryThreshold.reset();
        concurrencyThreshold.reset();
    }

    @Override
    public void submit(RedisOpCommand<?> command, long commandOffsetToAccumulate) {

        if (closeState.isClosed()) {
           logger.debug("[submit] already closed");
           return;
        }

        long bytes = command.redisOp().estimatedSize();
        memoryThreshold.tryPass(bytes);
        concurrencyThreshold.tryPass();

        if (bytesPerSecondThreshold != null) {
            bytesPerSecondThreshold.tryPass(bytes);
        }

        if (qpsThreshold != null) {
            qpsThreshold.tryPass();
        }

        stateThread.execute(() -> {
            if (logger.isDebugEnabled()) {
                logger.debug("[submit] commandName={} args={}", command.getName(), Arrays.stream(command.redisOp().buildRawOpArgs()).map(String::new).toArray(String[]::new));
            }
            switch (command.type()) {
                case SINGLE_KEY:
                    submitSingleKeyCommand((RedisOpDataCommand<?>) command, commandOffsetToAccumulate);
                    break;
                case MULTI_KEY:
                    submitMultiKeyCommand((RedisOpDataCommand<?>) command, commandOffsetToAccumulate);
                    break;
                case NONE_KEY:
                    submitNoneKeyCommand(command, commandOffsetToAccumulate);
                    break;
                case OTHER:
                    submitObstacle(command, commandOffsetToAccumulate);
                    break;
            }
        });
    }

    private Command<?> wrapWithRetry(RedisOpCommand<?> command) {
        return command.needGuaranteeSuccess() ? new StubbornCommand<>(command, workerThreads) : command;
    }

    private void submitNoneKeyCommand(RedisOpCommand<?> command, long commandOffset) {

        /* find dependencies */

        List<SequenceCommand<?>> dependencies = new ArrayList<>();

        if (obstacle != null) {
            dependencies.add(obstacle);
        }

        /* make command */

        SequenceCommand<?> current = new SequenceCommand<>(dependencies, wrapWithRetry(command), stateThread, workerThreads);

        /* do some stuff when finish */

        mergeGtidWhenSuccess(current, command.gtid());
        releaseMemoryThresholdWhenDone(current, command.redisOp().estimatedSize());
        increaseOffsetWhenSuccess(current, commandOffset);

        /* run self */

        current.execute();
    }

    private void submitSingleKeyCommand(RedisOpDataCommand<?> command, long commandOffset) {

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

        SequenceCommand<?> current = new SequenceCommand<>(dependencies, wrapWithRetry(command), stateThread, workerThreads);

        /* make self a dependency */

        runningCommands.put(key, current);
        forgetWhenDone(current, key);

        /* do some stuff when finish */

        mergeGtidWhenSuccess(current, command.gtid());
        releaseMemoryThresholdWhenDone(current, command.redisOp().estimatedSize());
        increaseOffsetWhenSuccess(current, commandOffset);

        /* run self */

        current.execute();
    }

    private void submitMultiKeyCommand(RedisOpDataCommand<?> command, long commandOffset) {

        List<RedisKey> keys = command.keys();
        List<SequenceCommand<?>> dependencies = keys.stream().map(runningCommands::get).filter(Objects::nonNull).collect(Collectors.toList());
        if (obstacle != null) {
            dependencies.add(obstacle);
        }

        SequenceCommand<?> current = new SequenceCommand<>(dependencies, wrapWithRetry(command), stateThread, workerThreads);

        for (RedisKey key : keys) {
            runningCommands.put(key, current);
            forgetWhenDone(current, key);
        }

        mergeGtidWhenSuccess(current, command.gtid());
        releaseMemoryThresholdWhenDone(current, command.redisOp().estimatedSize());
        increaseOffsetWhenSuccess(current, commandOffset);

        current.execute();
    }

    private void submitObstacle(RedisOpCommand<?> command, long commandOffset) {

        /* find dependencies */

        List<SequenceCommand<?>> dependencies = new ArrayList<>(runningCommands.values());
        if (obstacle != null) {
            dependencies.add(obstacle);
        }

        /* make command */

        SequenceCommand<?> current = new SequenceCommand<>(dependencies, wrapWithRetry(command), stateThread, workerThreads);

        /* make self a dependency */

        runningCommands = new HashMap<>();
        obstacle = current;

        forgetObstacleWhenDone(current);

        /* do some stuff when finish */

        mergeGtidWhenSuccess(current, command.gtid());
        releaseMemoryThresholdWhenDone(current, command.redisOp().estimatedSize());
        increaseOffsetWhenSuccess(current, commandOffset);

        /* run self */

        current.execute();
    }

    private void forgetObstacleWhenDone(SequenceCommand<?> sequenceCommand) {
        sequenceCommand.future().addListener((f) -> {
            if (sequenceCommand == obstacle) {
                obstacle = null;
            }
        });
    }

    private void forgetWhenDone(SequenceCommand<?> sequenceCommand, RedisKey key) {
        sequenceCommand.future().addListener((f) -> {
            if (sequenceCommand == runningCommands.get(key)) {
                runningCommands.remove(key);
            }
        });
    }

    private void mergeGtidWhenSuccess(SequenceCommand<?> sequenceCommand, String gtid) {
        sequenceCommand.future().addListener((f) -> {
            if (f.isSuccess()) {
                if (gtid != null) {
                    if (lwmManager != null) {
                        lwmManager.submit(gtid);
                    }
                }
            }
        });
    }

    private void releaseMemoryThresholdWhenDone(SequenceCommand<?> sequenceCommand, long memory) {
        sequenceCommand.future().addListener((f)->{
            concurrencyThreshold.release();
            memoryThreshold.release(memory);
        });
    }

    private void increaseOffsetWhenSuccess(SequenceCommand<?> sequenceCommand, long commandOffset) {
        sequenceCommand.future().addListener((f) -> {
            if (f.isSuccess()) {
                offsetRecorder.addAndGet(commandOffset);
            }
        });
    }
}
