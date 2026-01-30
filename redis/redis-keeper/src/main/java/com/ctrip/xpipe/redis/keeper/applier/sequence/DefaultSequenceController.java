package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.keeper.applier.AbstractInstanceComponent;
import com.ctrip.xpipe.redis.keeper.applier.ApplierStatistic;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import com.ctrip.xpipe.redis.keeper.applier.command.*;
import com.ctrip.xpipe.redis.keeper.applier.threshold.BytesPerSecondThreshold;
import com.ctrip.xpipe.redis.keeper.applier.threshold.ConcurrencyThreshold;
import com.ctrip.xpipe.redis.keeper.applier.threshold.MemoryThreshold;
import com.ctrip.xpipe.redis.keeper.applier.threshold.QPSThreshold;
import com.ctrip.xpipe.utils.CloseState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author Slight
 * <p>
 * Jan 29, 2022 4:19 PM
 */
public class DefaultSequenceController extends AbstractInstanceComponent implements ApplierSequenceController {

    @InstanceDependency
    public ExecutorService stateThread;

    @InstanceDependency
    public ScheduledExecutorService workerThreads;

    @InstanceDependency
    public ScheduledExecutorService scheduled;

    @InstanceDependency
    public AtomicLong offsetRecorder;

    @InstanceDependency
    public AtomicReference<GtidSet> execGtidSet;

    @InstanceDependency
    public AtomicReference<ApplierStatistic> applierStatisticRef;

    @InstanceDependency
    public AsyncRedisClient client;

    public MemoryThreshold memoryThreshold;

    public ConcurrencyThreshold concurrencyThreshold;

    public QPSThreshold qpsThreshold;

    public BytesPerSecondThreshold bytesPerSecondThreshold;

    public ConcurrencyThreshold batchConcurrencyThreshold;


    Map<RedisKey, SequenceCommand<?>> runningCommands = new HashMap<>();
    Map<RedisKey, List<byte[]>> batchRedisOpCommands = new HashMap<>();

    SequenceCommand<?> obstacle;

    private long qpsThresholdValue;

    private long bytesPerSecondThresholdValue;

    private int batchSize;

    public static final long DEFAULT_QPS_THRESHOLD = 50000000;

    // 100MB
    public static final long DEFAULT_BYTES_PER_SECOND_THRESHOLD = 100 * 1024 * 1024;

    // 32MB
    public static final long DEFAULT_MEMORY_THRESHOLD = 32 * 1024 * 1024;

    public static final long DEFAULT_CONCURRENCY_THRESHOLD = 1000;

    private final CloseState closeState = new CloseState();

    private static final int DEFAULT_BATCH_SIZE = 100;

    public DefaultSequenceController() {
        this(DEFAULT_QPS_THRESHOLD, DEFAULT_BYTES_PER_SECOND_THRESHOLD, DEFAULT_MEMORY_THRESHOLD, DEFAULT_CONCURRENCY_THRESHOLD,DEFAULT_BATCH_SIZE);
    }

    public DefaultSequenceController(Long qpsThreshold, Long bytesPerSecondThreshold, Long memoryThreshold, Long concurrencyThreshold,int batchSize) {
        this.qpsThresholdValue = qpsThreshold == null ? DEFAULT_QPS_THRESHOLD : qpsThreshold;
        this.bytesPerSecondThresholdValue = bytesPerSecondThreshold == null ? DEFAULT_BYTES_PER_SECOND_THRESHOLD : bytesPerSecondThreshold;
        this.memoryThreshold = new MemoryThreshold(memoryThreshold == null ? DEFAULT_MEMORY_THRESHOLD : memoryThreshold);
        this.concurrencyThreshold = new ConcurrencyThreshold(concurrencyThreshold == null ? DEFAULT_CONCURRENCY_THRESHOLD : concurrencyThreshold);
        this.batchConcurrencyThreshold = new ConcurrencyThreshold(concurrencyThreshold == null ? DEFAULT_CONCURRENCY_THRESHOLD * batchSize : concurrencyThreshold * batchSize);
        this.batchSize = batchSize != 0 ? batchSize : DEFAULT_BATCH_SIZE;
    }

    @Override
    protected void doInitialize() throws Exception {
        qpsThreshold = new QPSThreshold(qpsThresholdValue, scheduled, true, "DefaultSequenceController");
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
        submit(command, commandOffsetToAccumulate, null);
    }

    @Override
    public void submit(RedisOpCommand<?> command, long commandOffsetToAccumulate, GtidSet gtidSet) {

        if (closeState.isClosed()) {
           logger.debug("[submit] already closed");
           return;
        }

        long bytes = command.redisOp().estimatedSize();
        memoryThreshold.tryPass(bytes);

        RedisOp redisOp = command.redisOp();
        RedisOpType redisOpType = redisOp.getOpType();
        if(commandOffsetToAccumulate == 0) {
            switch (redisOpType) {
                case RedisOpType.RPUSH:
                case RedisOpType.SADD:
                case RedisOpType.HSET:
                case RedisOpType.ZADD:
                    batchConcurrencyThreshold.tryPass();
                    break;
                default:
                    concurrencyThreshold.tryPass();
            }
        }else {
            concurrencyThreshold.tryPass();
        }

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
                    if(commandOffsetToAccumulate == 0) {
                        switch (redisOpType) {
                            case RedisOpType.RPUSH:
                            case RedisOpType.SADD:
                                submitListAndSetBatchCommand((DefaultDataCommand) command, commandOffsetToAccumulate, gtidSet);
                                break;
                            case RedisOpType.HSET:
                            case RedisOpType.ZADD:
                                submitHashAndZsetBatchCommand((DefaultDataCommand) command, commandOffsetToAccumulate, gtidSet);
                                break;
                            default:
                                submitSingleKeyCommand((RedisOpDataCommand<?>) command, commandOffsetToAccumulate, gtidSet);
                        }
                    }else {
                        submitSingleKeyCommand((RedisOpDataCommand<?>) command, commandOffsetToAccumulate, gtidSet);
                    }
                    break;
                case MULTI_KEY:
                    submitMultiKeyCommand((RedisOpDataCommand<?>) command, commandOffsetToAccumulate, gtidSet);
                    break;
                case NONE_KEY:
                    submitNoneKeyCommand(command, commandOffsetToAccumulate, gtidSet);
                    break;
                case OTHER:
                    submitObstacle(command, commandOffsetToAccumulate, gtidSet);
                    break;
            }
        });
    }

    private Command<?> wrapWithRetry(RedisOpCommand<?> command) {
        if (command.needGuaranteeSuccess()) {
            StubbornCommand<?> wrap = new StubbornCommand<>(command, workerThreads);
            wrap.setStatistic(applierStatisticRef.get());
            return wrap;
        } else {
            return command;
        }
    }

    private void submitNoneKeyCommand(RedisOpCommand<?> command, long commandOffset, GtidSet gtidSet) {

        /* find dependencies */

        List<SequenceCommand<?>> dependencies = new ArrayList<>();

        if (obstacle != null) {
            dependencies.add(obstacle);
        }

        /* make command */

        SequenceCommand<?> current = new SequenceCommand<>(dependencies, wrapWithRetry(command), stateThread, workerThreads);

        /* do some stuff when finish */

        increaseOffsetWhenSuccessAndGtidSet(current, commandOffset, gtidSet);
        releaseMemoryThresholdWhenDone(current, command.redisOp().estimatedSize());

        /* run self */

        current.execute();
    }

    private void submitSingleKeyCommand(RedisOpDataCommand<?> command, long commandOffset, GtidSet gtidSet) {

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

        increaseOffsetWhenSuccessAndGtidSet(current, commandOffset, gtidSet);
        releaseMemoryThresholdWhenDone(current, command.redisOp().estimatedSize());

        /* run self */

        current.execute();
    }

    private void submitListAndSetBatchCommand(DefaultDataCommand command, long commandOffset, GtidSet gtidSet) {
        RedisKey key = command.key();
        RedisOpSingleKey redisOp = (RedisOpSingleKey) command.redisOp();
        final byte[][] args = redisOp.buildRawOpArgs();
        List<byte[]> lastBatchArgs = batchRedisOpCommands.computeIfAbsent(key,(redisKey) -> {
            List<byte[]> batchArgs = new ArrayList<>(2+batchSize);
            batchArgs.add(args[0]);
            batchArgs.add(args[1]);
            return batchArgs;
        });
        lastBatchArgs.add(args[2]);
        if(redisOp.isLastOp()|| lastBatchArgs.size() >= batchSize+2){
            RedisOp batchRedisOp = new RedisOpSingleKey(redisOp.getOpType(),lastBatchArgs.toArray(new byte[0][]),key,null);
            submitSingleKeyCommand(new DefaultDataCommand(client,batchRedisOp,command.getDbNumber()),commandOffset,gtidSet);
            lastBatchArgs.clear();
            lastBatchArgs.add(args[0]);
            lastBatchArgs.add(args[1]);
        }
        if(redisOp.isLastOp()){
            batchRedisOpCommands.remove(key);
        }
    }

    private void submitHashAndZsetBatchCommand(DefaultDataCommand command, long commandOffset, GtidSet gtidSet) {
        RedisKey key = command.key();
        RedisOpSingleKey redisOp = (RedisOpSingleKey) command.redisOp();
        final byte[][] args = redisOp.buildRawOpArgs();
        List<byte[]> lastBatchArgs = batchRedisOpCommands.computeIfAbsent(key,(redisKey) -> {
            List<byte[]> batchArgs = new ArrayList<>(2+batchSize*2);
            batchArgs.add(args[0]);
            batchArgs.add(args[1]);
            return batchArgs;
        });
        lastBatchArgs.add(args[2]);
        lastBatchArgs.add(args[3]);
        if(redisOp.isLastOp() ||lastBatchArgs.size() >= 2+batchSize*2){
            RedisOp batchRedisOp = new RedisOpSingleKey(redisOp.getOpType(),lastBatchArgs.toArray(new byte[0][]),key,null);
            submitSingleKeyCommand(new DefaultDataCommand(client,batchRedisOp,command.getDbNumber()),commandOffset,gtidSet);
            lastBatchArgs.clear();
            lastBatchArgs.add(args[0]);
            lastBatchArgs.add(args[1]);
        }
        if(redisOp.isLastOp()){
            batchRedisOpCommands.remove(key);
        }
    }



    private void submitMultiKeyCommand(RedisOpDataCommand<?> command, long commandOffset, GtidSet gtidSet) {

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

        increaseOffsetWhenSuccessAndGtidSet(current, commandOffset, gtidSet);
        releaseMemoryThresholdWhenDone(current, command.redisOp().estimatedSize());

        current.execute();
    }

    private void submitObstacle(RedisOpCommand<?> command, long commandOffset, GtidSet gtidSet) {

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

        increaseOffsetWhenSuccessAndGtidSet(current, commandOffset, gtidSet);
        releaseMemoryThresholdWhenDone(current, command.redisOp().estimatedSize());

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


    private void releaseMemoryThresholdWhenDone(SequenceCommand<?> sequenceCommand, long memory) {
        sequenceCommand.future().addListener((f) -> {
            concurrencyThreshold.release();
            memoryThreshold.release(memory);
        });
    }

    private void increaseOffsetWhenSuccessAndGtidSet(SequenceCommand<?> sequenceCommand, long commandOffset, GtidSet gtidSet) {
        sequenceCommand.future().addListener((f) -> {
            if (f.isSuccess()) {
                offsetRecorder.addAndGet(commandOffset);
                if(gtidSet != null && gtidSet.itemCnt() > 0) {
                    execGtidSet.get().intersectionGtidSet(gtidSet);
                }
            }
        });
    }

}
