package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.keeper.applier.AbstractInstanceComponent;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import com.ctrip.xpipe.redis.keeper.applier.command.*;
import com.ctrip.xpipe.redis.keeper.applier.lwm.ApplierLwmManager;

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
    public ScheduledExecutorService stateThread;

    Map<RedisKey, SequenceCommand<?>> runningCommands = new HashMap<>();

    SequenceCommand<?> obstacle;

    ExecutorService workerThreads;

    @Override
    protected void doInitialize() throws Exception {
        workerThreads = Executors.newFixedThreadPool(8);
    }

    @Override
    protected void doDispose() throws Exception {
        workerThreads.shutdown();
    }

    @Override
    public void submit(RedisOpCommand<?> command) {

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
                case MULTI:
                case EXEC:
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

        SequenceCommand<?> current = new SequenceCommand<>(dependencies, new StubbornCommand<>(command), stateThread, workerThreads);

        /* make self a dependency */

        runningCommands.put(key, current);
        forgetWhenSuccess(current, key);

        /* do some stuff when finish */

        mergeGtidWhenSuccess(current, command.gtid());

        /* run self */

        current.execute();
    }

    private void submitMultiKeyCommand(RedisOpDataCommand<?> command) {

        List<SequenceCommand<?>> currents = new ArrayList<>();

        for (RedisOpCommand<?> subCommand : command.sharding()) {

            /* find dependencies */

            List<RedisKey> keys = subCommand.keys();
            List<SequenceCommand<?>> dependencies = keys.stream().map(runningCommands::get).filter(Objects::nonNull).collect(Collectors.toList());
            if (obstacle != null) {
                dependencies.add(obstacle);
            }

            /* make command */

            SequenceCommand<?> current = new SequenceCommand<>(dependencies, new StubbornCommand<>(subCommand), stateThread, workerThreads);

            /* make self a dependency */

            for (RedisKey key : keys) {
                runningCommands.put(key, current);
                forgetWhenSuccess(current, key);
            }

            currents.add(current);
        }

        /* run self */

        for (SequenceCommand<?> current : currents) {
            current.execute();
        }

        /* do some stuff when finish */

        SequenceCommand<?> success = new SuccessSequenceCommand(currents, stateThread, workerThreads);
        mergeGtidWhenSuccess(success, command.gtid());

        success.execute();
    }

    private void submitObstacle(RedisOpCommand<?> command) {

        /* find dependencies */

        List<SequenceCommand<?>> dependencies = new ArrayList<>(runningCommands.values());
        if (obstacle != null) {
            dependencies.add(obstacle);
        }

        /* make command */

        SequenceCommand<?> current = new SequenceCommand<>(dependencies, new StubbornCommand<>(command), stateThread, workerThreads);

        /* make self a dependency */

        runningCommands = new HashMap<>();
        obstacle = current;

        forgetObstacleWhenSuccess(current);

        /* do some stuff when finish */

        mergeGtidWhenSuccess(current, command.gtid());

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
}
