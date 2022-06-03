package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.keeper.applier.AbstractInstanceComponent;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import com.ctrip.xpipe.redis.keeper.applier.command.*;
import com.ctrip.xpipe.redis.keeper.applier.lwm.ApplierLwmManager;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * @author Slight
 * <p>
 * Jan 29, 2022 4:19 PM
 */
public class DefaultSequenceController extends AbstractInstanceComponent implements ApplierSequenceController {

    @InstanceDependency
    public ApplierLwmManager lwmManager;

    final Map<RedisKey, SequenceCommand<?>> runningCommands = new HashMap<>();

    ExecutorService stateThread;
    ExecutorService workerThreads;

    @Override
    protected void doInitialize() throws Exception {
        stateThread = Executors.newSingleThreadExecutor();
        workerThreads = Executors.newFixedThreadPool(8);
    }

    @Override
    protected void doDispose() throws Exception {
        stateThread.shutdown();
        workerThreads.shutdown();
    }

    @Override
    public void submit(RedisOpDataCommand<?> command) {

        if (command.type() == RedisOpCommandType.SINGLE_KEY) {
            stateThread.execute(()->{
                submitSingleKeyCommand(command);
            });
        }

        if (command.type() == RedisOpCommandType.MULTI_KEY) {
            stateThread.execute(()->{
                submitMultiKeyCommand(command);
            });
        }

        //List<RedisKey> keys = command.keys();
        //List<SequenceCommand<?>> pasts = keys.stream().map(runningCommands::get).collect(Collectors.toList());
        //SequenceCommand<?> sequenceCommand = new SequenceCommand<>(pasts, new StubbornCommand<>(command), stateThread, workerThreads);

        //CommandFuture<?> current = runningCommands.get(key);

        //if (current == null) {
        //    runningCommands.put(key, sequenceCommand.execute());
        //    return;
        //}

        //if (current.isDone()) {
        //    if (current.isSuccess()) {
        //        runningCommands.put(key, sequenceCommand.execute());
        //    } else {
        //        current.command().reset();
        //        current = current.command().execute();
        //    }
        //}
    }

    private void submitSingleKeyCommand(RedisOpDataCommand<?> command) {
        RedisKey key = command.key();
        SequenceCommand<?> running = runningCommands.get(key);
        SequenceCommand<?> current = running == null ?
                new SequenceCommand<>(new StubbornCommand<>(command), stateThread, workerThreads) :
                new SequenceCommand<>(running, new StubbornCommand<>(command), stateThread, workerThreads);

        runningCommands.put(key, current);
        forgetWhenSuccess(current, key);
        mergeGtidWhenSuccess(current, command.gtid());
        current.execute();
    }

    private void submitMultiKeyCommand(RedisOpDataCommand<?> command) {

        List<SequenceCommand<?>> currents = new ArrayList<>();

        for (RedisOpCommand<?> subCommand : command.sharding()) {
            List<RedisKey> keys = subCommand.keys();
            List<SequenceCommand<?>> running = keys.stream().map(runningCommands::get).filter(Objects::nonNull).collect(Collectors.toList());
            SequenceCommand<?> current = new SequenceCommand<>(running, new StubbornCommand<>(subCommand), stateThread, workerThreads);

            for (RedisKey key : keys) {
                runningCommands.put(key, current);
                forgetWhenSuccess(current, key);
            }

            currents.add(current);
        }

        for (SequenceCommand<?> current : currents) {
            current.execute();
        }

        SequenceCommand<?> success = new SuccessSequenceCommand(currents, stateThread, workerThreads);
        mergeGtidWhenSuccess(success, command.gtid());

        success.execute();
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
