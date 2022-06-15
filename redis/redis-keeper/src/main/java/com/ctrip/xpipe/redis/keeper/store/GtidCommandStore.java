package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.store.cmd.GtidSetCommandWriter;
import com.ctrip.xpipe.redis.keeper.store.cmd.GtidSetReplicationProgress;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.function.IntSupplier;

/**
 * @author lishanglin
 * date 2022/5/24
 */
public class GtidCommandStore extends AbstractCommandStore<GtidSetReplicationProgress, RedisOp>
        implements CommandStore<GtidSetReplicationProgress, RedisOp> {

    private static final Logger logger = LoggerFactory.getLogger(GtidCommandStore.class);

    public GtidCommandStore(File file, int maxFileSize, GtidSet baseGtidSet, IntSupplier maxTimeSecondKeeperCmdFileAfterModified,
                            int minTimeMilliToGcAfterModified, IntSupplier fileNumToKeep, long commandReaderFlyingThreshold,
                            CommandReaderWriterFactory<GtidSetReplicationProgress, RedisOp> cmdReaderWriterFactory,
                            KeeperMonitor keeperMonitor) throws IOException {
        super(file, maxFileSize, maxTimeSecondKeeperCmdFileAfterModified, minTimeMilliToGcAfterModified, fileNumToKeep,
                commandReaderFlyingThreshold, baseGtidSet, cmdReaderWriterFactory, keeperMonitor);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public void addCommandsListener(GtidSetReplicationProgress progress, CommandsListener listener) throws IOException {
        makeSureOpen();
        logger.info("[addCommandsListener][begin] from gtidset {}, {}", progress, listener);

        CommandReader<RedisOp> cmdReader = null;

        try {
            cmdReader = beginRead(progress);
        } finally {
            // ensure beforeCommand() is always called
            listener.beforeCommand();
        }

        logger.info("[addCommandsListener] from {}, {}", progress, cmdReader);

        try {
            while (listener.isOpen() && !Thread.currentThread().isInterrupted()) {

                final RedisOp redisOp = cmdReader.read();
                if (null == redisOp) continue;

                logger.debug("[addCommandsListener] {}", redisOp);

                // TODO: monitor send delay
//                if(getDelayTraceLogger().isDebugEnabled()){
//                    getDelayTraceLogger().debug("[write][begin]{}, {}", listener, referenceFileRegion.getTotalPos());
//                }
//                getCommandStoreDelay().beginSend(listener, referenceFileRegion.getTotalPos());

                ChannelFuture future = listener.onCommand(redisOp);

                if(future != null){
                    CommandReader<RedisOp> finalCmdReader = cmdReader;
                    future.addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {

                            finalCmdReader.flushed(redisOp);
//                            getCommandStoreDelay().flushSucceed(listener, referenceFileRegion.getTotalPos());
//                            if(logger.isDebugEnabled()){
//                                getDelayTraceLogger().debug("[write][ end ]{}, {}", listener, referenceFileRegion.getTotalPos());
//                            }
                        }
                    });
                }
            }
        } catch (Throwable th) {
            logger.error("[readCommands][exit]" + listener, th);
        } finally {
            cmdReader.close();
        }
        logger.info("[addCommandsListener][end] from {}, {}", progress, listener);
    }

    public void setBaseGtidSet(String baseGtidSet) {
        this.baseGtidSet = new GtidSet(baseGtidSet);
    }

    @Override
    public GtidSet getEndGtidSet() {
        makeSureOpen();
        return ((GtidSetCommandWriter)getCmdWriter()).getGtidSetContain();
    }
}
