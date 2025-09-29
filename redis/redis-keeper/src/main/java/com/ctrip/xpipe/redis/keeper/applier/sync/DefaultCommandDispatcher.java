package com.ctrip.xpipe.redis.keeper.applier.sync;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.payload.InOutPayloadFactory;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.cmd.RedisProtocolParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.keeper.applier.AbstractInstanceComponent;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import com.ctrip.xpipe.redis.keeper.applier.command.*;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.threshold.GTIDDistanceThreshold;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol.ASTERISK_BYTE;
import static com.ctrip.xpipe.redis.core.protocal.Sync.SYNC_STATE.READING_COMMANDS;

/**
 * @author Slight
 * <p>
 * Jun 05, 2022 18:21
 */
public class DefaultCommandDispatcher extends AbstractInstanceComponent implements ApplierCommandDispatcher {
    @InstanceDependency
    public ApplierSequenceController sequenceController;

    @InstanceDependency
    public AsyncRedisClient client;

    @InstanceDependency
    public RedisOpParser parser;

    @InstanceDependency
    public ExecutorService stateThread;

    @InstanceDependency
    public ScheduledExecutorService workerThreads;

    @InstanceDependency
    public AtomicReference<GtidSet> execGtidSet;

    @InstanceDependency
    public AtomicReference<GtidSet> startGtidSet;

    @InstanceDependency
    public AtomicReference<GtidSet> lostGtidSet;


    @InstanceDependency
    public AtomicReference<GTIDDistanceThreshold> gtidDistanceThreshold;

    @InstanceDependency
    public AtomicLong offsetRecorder;

    @InstanceDependency
    public AtomicReference<String> replId;

    // in order to aggregate the entire transaction into one command
    private AtomicReference<TransactionCommand> transactionCommand;

    private int dbNumber = 0;

    private String startGtidStr = "";
    private String lostGtidStr = "";
    private long startOffset = -1;
    private RedisProtocolParser redisProtocolParser;

    public DefaultCommandDispatcher() {

        this.transactionCommand = new AtomicReference<>();
    }

    @VisibleForTesting
    void resetState() {
        this.gtidDistanceThreshold.set(new GTIDDistanceThreshold(2000));
        this.transactionCommand.set(null);
        this.execGtidSet.set(new GtidSet(GtidSet.EMPTY_GTIDSET));
        startGtidStr = "";
        startOffset = -1;
        lostGtidStr = "";
    }

    @Override
    public void doOnFullSync(String replId, long replOff) {
        this.replId.set(replId);
        this.startOffset = replOff;
    }

    @Override
    public void doOnXFullSync(GtidSet lost, long replOff) {
        this.lostGtidStr = lost.toString();
        this.startOffset = replOff;
    }

    @Override
    public void doOnXContinue(GtidSet lost, long replOffset) {
        this.offsetRecorder.set(replOffset);
        this.resetState();
    }

    @Override
    public void doOnContinue(String newReplId) {
        replId.set(newReplId);
        this.resetState();
    }

    @Override
    public void doOnAppendCommand(ByteBuf byteBuf) {
        if(redisProtocolParser == null) {
            redisProtocolParser = new RedisProtocolParser();
        }

        Object cmdPayload = redisProtocolParser.parse(byteBuf);
        if (cmdPayload instanceof Object[]) {
            RedisOp redisOp = null;
            try {
                Object[] rawCmdArgs = (Object[]) cmdPayload;
                redisOp = parser.parse(rawCmdArgs);
                doOnRedisOp(redisOp, redisProtocolParser.getCurrentCommandOffset(), redisOp.getOpGtid());
                redisProtocolParser.reset();
            } catch (Throwable unlikely) {
                try {
                    logger.error("[onCommand] unlikely - when doing partial sync]", unlikely);
                    logger.error("[onCommand] unlikely {}", redisOp);
                } catch (Throwable ignore) {
                }
            }
        } else if (null != cmdPayload) {
            logger.info("[doReceiveResponse][{}][unknown payload] {}, {}", READING_COMMANDS, this, cmdPayload);
            throw new RedisRuntimeException("unknown payload:" + cmdPayload);
        }
    }

    @Override
    public void endReadRdb() {
        logger.info("[endReadRdb] start {}, lost: {}, offset: {} ", startGtidStr, lostGtidStr, startOffset);
        this.offsetRecorder.set(startOffset);
        this.lostGtidSet.set(new GtidSet(lostGtidStr));
        this.startGtidSet.set(new GtidSet(startGtidStr));
    }

    @Override
    public void protoChange() {

    }

    @Override
    public GtidSet getGtidReceived() {
        GtidSet ref = execGtidSet.get();
        if (ref != null) {
            return ref.clone();
        }
        return null;
    }

    protected int toInt(byte[] value) {
        int rt = 0;
        for (byte b : value) {
            int add = b - '0';
            rt = rt * 10;
            rt += add;
        }
        return rt;
    }

    private void addTransactionStart(RedisOpCommand<?> multiCommand, long commandOffsetToAccumulate, String gtid) {
        transactionCommand.set(new TransactionCommand());
        transactionCommand.get().addTransactionStart(multiCommand, commandOffsetToAccumulate, gtid);
    }

    private void addTransactionEndAndSubmit(RedisOpCommand<?> execCommand, long commandOffsetToAccumulate, String gtid) {
        transactionCommand.get().addTransactionEnd(execCommand, commandOffsetToAccumulate, gtid);
        TransactionCommand command = transactionCommand.getAndSet(null);
        sequenceController.submit(command, command.commandOffset(), command.getGtidSet());
    }

    private void addIfTransactionCommandsOrSubmit(RedisOpCommand<?> redisOpCommand, long commandOffsetToAccumulate, String gtid) {
        if (transactionCommand.get() != null) {
            transactionCommand.get().addTransactionCommands(redisOpCommand, commandOffsetToAccumulate, gtid);
        } else {
            GtidSet gtidSet = new GtidSet(GtidSet.EMPTY_GTIDSET);
            gtidSet.add(gtid);
            sequenceController.submit(redisOpCommand, commandOffsetToAccumulate, gtidSet);
        }
    }

    @VisibleForTesting
    protected boolean shouldFilter(RedisOp redisOp) {
        if (RedisOpType.PUBLISH.equals(redisOp.getOpType())) {
            int length = redisOp.buildRawOpArgs().length;
            String channel;
            if (length == 3) {
                channel = new String(redisOp.buildRawOpArgs()[1]);
            } else if (length >= 5) {
                channel = new String(redisOp.buildRawOpArgs()[4]);
            } else {
                logger.warn("publish command {} length={} unexpected, filtered", redisOp, length);
                return true;
            }
            if (!channel.startsWith("xpipe-asymmetric-")) {
                logger.warn("publish command {} channel: [{}] filtered", redisOp, channel);
                return true;
            }
        }
        if (redisOp.getOpType().isSwallow()) {
            logger.info("[onRedisOp] swallow redisOp: {}", redisOp.toString());
            EventMonitor.DEFAULT.logEvent("APPLIER.SWALLOW.OP", getCmd(redisOp));
        }
        return redisOp.getOpType().isSwallow();
    }

    private String getCmd(RedisOp redisOp) {
        byte[][] rawOpArgs = redisOp.buildRawOpArgs();
        if (Objects.isNull(redisOp.getOpGtid()) && rawOpArgs.length > 0) {
            return new String(redisOp.buildRawOpArgs()[0]);
        }
        if (!Objects.isNull(redisOp.getOpGtid()) && rawOpArgs.length > 3) {
            return new String(redisOp.buildRawOpArgs()[3]);
        }
        return redisOp.getOpType().name();
    }

    private void doOnRedisOp(RedisOp redisOp, long commandOffsetToAccumulate, String gtid) {
        logger.debug("[onRedisOp] redisOpType={}, gtid={}", redisOp.getOpType(), redisOp.getOpGtid());

        redisOp.clearGtid();
        if (RedisOpType.PING.equals(redisOp.getOpType())) {
            offsetRecorder.addAndGet(commandOffsetToAccumulate);
            return;
        }
        if (RedisOpType.SELECT.equals(redisOp.getOpType())) {
            try {
                dbNumber = toInt(redisOp.buildRawOpArgs()[1]);
            } catch (Throwable unlikely) {
                logger.error("[onRedisOp] unlikely - fail to select db : {}", Arrays.toString(redisOp.buildRawOpArgs()[1]));
                logger.error("[onRedisOp] unlikely - fail to select db]", unlikely);
            }
            offsetRecorder.addAndGet(commandOffsetToAccumulate);
            return;
        }

        if (shouldFilter(redisOp)) {
            offsetRecorder.addAndGet(commandOffsetToAccumulate);
            return;
        }

        if (RedisOpType.MULTI.equals(redisOp.getOpType())) {
            addTransactionStart(new DefaultMultiCommand(client, redisOp), commandOffsetToAccumulate, gtid);
        } else if (RedisOpType.EXEC.equals(redisOp.getOpType())) {
            addTransactionEndAndSubmit(new DefaultExecCommand(client, redisOp), commandOffsetToAccumulate, gtid);
        } else if (redisOp instanceof RedisMultiKeyOp) {
            addIfTransactionCommandsOrSubmit(new MultiDataCommand(client, (RedisMultiKeyOp) redisOp, dbNumber, workerThreads),
                    commandOffsetToAccumulate, gtid);
        } else {
            addIfTransactionCommandsOrSubmit(new DefaultDataCommand(client, redisOp, dbNumber),
                    commandOffsetToAccumulate, gtid);
        }
    }

    @Override
    public void onRedisOp(RedisOp redisOp) {
        doOnRedisOp(redisOp, 0, null);
    }

    @Override
    public void onAux(String key, String value) {
    }

    @Override
    public void onFinish(RdbParser<?> parser) {

    }

    @Override
    public void onAuxFinish(Map<String, String> auxMap) {
        startGtidStr = auxMap.getOrDefault(RdbConstant.REDIS_RDB_AUX_KEY_GTID_EXECUTED, GtidSet.EMPTY_GTIDSET);
        lostGtidStr = auxMap.getOrDefault(RdbConstant.REDIS_RDB_AUX_KEY_GTID_LOST, GtidSet.EMPTY_GTIDSET);
        logger.info("[onAuxFinish] startGtidStr: {}, lostGtidStr: {}", startGtidSet, lostGtidStr);
    }
}