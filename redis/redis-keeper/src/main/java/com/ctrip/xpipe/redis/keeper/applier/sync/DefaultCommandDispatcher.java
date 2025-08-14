package com.ctrip.xpipe.redis.keeper.applier.sync;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
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
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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

    @VisibleForTesting
    Set<String> receivedSids;

    private RedisClientProtocol<Object[]> protocolParser = new ArrayParser();

    // in order to aggregate the entire transaction into one command
    private AtomicReference<TransactionCommand> transactionCommand;

    private int dbNumber = 0;

    private String startGtidStr = "";
    private String lostGtidStr = "";
    private long startOffset = -1;

    public DefaultCommandDispatcher() {

        this.receivedSids = new HashSet<>();

        this.transactionCommand = new AtomicReference<>();
    }

    @VisibleForTesting
    void resetState() {
        this.receivedSids = new HashSet<>();
        this.gtidDistanceThreshold.set(new GTIDDistanceThreshold(2000));
        this.transactionCommand.set(null);
        this.execGtidSet.set(new GtidSet(GtidSet.EMPTY_GTIDSET));
        startGtidStr = "";
        startOffset = -1;
        lostGtidStr = "";
    }

    @Override
    public void doOnFullSync(long replOff) {
        this.startOffset = replOff;
    }

    @Override
    public void doOnXFullSync(GtidSet lost, long replOff) {
        this.lostGtidStr = lost.toString();
        this.startOffset = replOff;
    }

    @Override
    public void doOnXContinue(GtidSet lost, long replOffset) {
        this.resetState();
    }

    @Override
    public void doOnContinue(String newReplId) {
        replId.set(newReplId);
        this.resetState();
    }

    @Override
    public void doOnAppendCommand(ByteBuf byteBuf) {
        while (byteBuf.readableBytes() > 0) {
            int pre = byteBuf.readerIndex();
            RedisClientProtocol<Object[]> protocol = protocolParser.read(byteBuf);
            if (protocol == null) {
                this.protocolParser.reset();
                break;
            }
            Object[] payload = protocol.getPayload();
            RedisOp redisOp = parser.parse(payload);
            String gtid = redisOp.getOpGtid();
            doOnRedisOp(redisOp, byteBuf.readerIndex() - pre);
            if(!StringUtil.isEmpty(gtid)) {
                execGtidSet.get().add(gtid);
            }
            this.protocolParser.reset();
        }
    }

    @Override
    public void endReadRdb() {
        logger.info("[endReadRdb]{}", startGtidStr);
        this.offsetRecorder.set(startOffset);
        this.lostGtidSet.set(new GtidSet(lostGtidStr));
        this.startGtidSet.set(new GtidSet(startGtidStr));
    }

    @Override
    public GtidSet getGtidReceived() {
        GtidSet ref = execGtidSet.get();
        if (ref != null) {
            return ref.clone();
        }
        return null;
    }

    private class GtidRiseJob implements Runnable {

        private final String gtid;

        private GtidRiseJob(String gtid) {
            this.gtid = gtid;
        }

        @Override
        public void run() {
            logger.debug("[updateGtidState] rise gtid {} to gtid_executed {}", gtid, execGtidSet.get());
            execGtidSet.get().rise(gtid);
        }
    }

    private class GtidCompensateJob implements Runnable {

        private final String sourceId;

        private final long last;

        private final long current;

        public GtidCompensateJob(String sourceId, long last, long current) {
            this.sourceId = sourceId;
            this.last = last;
            this.current = current;
        }

        @Override
        public void run() {
            logger.debug("[updateGtidState] add leap gtid {}:{}-{} to gtid_executed {}", sourceId, last + 1, current - 1, execGtidSet.get());
            execGtidSet.get().compensate(sourceId, last + 1, current - 1);
        }
    }

    public boolean /* skip? */ updateGtidState(String gtid) {

        if (null == gtid) {
            return false;
        }

        Pair<String, Long> parsed = Objects.requireNonNull(GtidSet.parseGtid(gtid));

        if (receivedSids.add(parsed.getKey())) {
            //sid first received
            execGtidSet.get().rise(gtid);

            stateThread.execute(new GtidRiseJob(gtid));
        } else {
            //sid already received, transactionId may leap
            long last = execGtidSet.get().rise(gtid);
            long current = parsed.getValue();
            if (current <= last) {
                //gtid under low watermark
                return true;
            }
            if (current - last > 10000) {
                logger.info("[updateGtidState] gtid leap a lot - last: {}, current: {}, gtid: {}, gtid_received: {}", last, current, gtid, execGtidSet.toString());
            }
            if (current > last + 1) {
                stateThread.execute(new GtidCompensateJob(parsed.getKey(), last, current));
            }
        }

        try {
            gtidDistanceThreshold.get().tryPass(execGtidSet.get().lwmSum());
        } catch (InterruptedException interrupted) {
            logger.info("[updateGtidState] gtidDistanceThreshold.tryPass() interrupted, probably quit.");
            throw new XpipeRuntimeException("gtidDistanceThreshold.tryPass() interrupted, probably quit", interrupted);
        }
        return false;
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

    private void addTransactionStart(RedisOpCommand<?> multiCommand, long commandOffsetToAccumulate) {
        transactionCommand.set(new TransactionCommand());
        transactionCommand.get().addTransactionStart(multiCommand, commandOffsetToAccumulate);
    }

    private void addTransactionEndAndSubmit(RedisOpCommand<?> execCommand, long commandOffsetToAccumulate) {
        transactionCommand.get().addTransactionEnd(execCommand, commandOffsetToAccumulate);
        TransactionCommand command = transactionCommand.getAndSet(null);
        sequenceController.submit(command, command.commandOffset());
    }

    private void addIfTransactionCommandsOrSubmit(RedisOpCommand<?> redisOpCommand, long commandOffsetToAccumulate) {
        if (transactionCommand.get() != null) {
            transactionCommand.get().addTransactionCommands(redisOpCommand, commandOffsetToAccumulate);
        } else {
            sequenceController.submit(redisOpCommand, commandOffsetToAccumulate);
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

    private void doOnRedisOp(RedisOp redisOp, long commandOffsetToAccumulate) {
        logger.debug("[onRedisOp] redisOpType={}, gtid={}", redisOp.getOpType(), redisOp.getOpGtid());

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
            addTransactionStart(new DefaultMultiCommand(client, redisOp), commandOffsetToAccumulate);
        } else if (RedisOpType.EXEC.equals(redisOp.getOpType())) {
            addTransactionEndAndSubmit(new DefaultExecCommand(client, redisOp), commandOffsetToAccumulate);
        } else if (redisOp instanceof RedisMultiKeyOp) {
            addIfTransactionCommandsOrSubmit(new MultiDataCommand(client, (RedisMultiKeyOp) redisOp, dbNumber, workerThreads), commandOffsetToAccumulate);
        } else {
            addIfTransactionCommandsOrSubmit(new DefaultDataCommand(client, redisOp, dbNumber), commandOffsetToAccumulate);
        }
    }

    @Override
    public void onRedisOp(RedisOp redisOp) {
        doOnRedisOp(redisOp, 0);
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
