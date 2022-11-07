package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.store.cmd.GtidSetCommandWriter;
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
public class GtidCommandStore extends DefaultCommandStore implements CommandStore {

    private static final Logger logger = LoggerFactory.getLogger(GtidCommandStore.class);

    public GtidCommandStore(File file, int maxFileSize, GtidSet baseGtidSet, IntSupplier maxTimeSecondKeeperCmdFileAfterModified,
                            int minTimeMilliToGcAfterModified, IntSupplier fileNumToKeep, long commandReaderFlyingThreshold,
                            CommandReaderWriterFactory cmdReaderWriterFactory,
                            KeeperMonitor keeperMonitor) throws IOException {
        super(file, maxFileSize, maxTimeSecondKeeperCmdFileAfterModified, minTimeMilliToGcAfterModified, fileNumToKeep,
                commandReaderFlyingThreshold, baseGtidSet, cmdReaderWriterFactory, keeperMonitor);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    private CommandReader<RedisOp> beginRead(GtidSetReplicationProgress replicationProgress) throws IOException {

        makeSureOpen();

        CommandReader<RedisOp> reader = cmdReaderWriterFactory.createCmdReader(replicationProgress, this,
                offsetNotifier, commandReaderFlyingThreshold);
        addReader(reader);
        return reader;
    }

    @Override
    public void addCommandsListener(ReplicationProgress<?> progress, CommandsListener listener) throws IOException {

        if (!(progress instanceof GtidSetReplicationProgress)) {
            super.addCommandsListener(progress, listener);
            return;
        }

        makeSureOpen();
        logger.info("[addCommandsListener][begin] from gtidset {}, {}", progress, listener);


        CommandReader<RedisOp> cmdReader = null;

        try {
            cmdReader = beginRead((GtidSetReplicationProgress) progress);
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
                ChannelFuture future = listener.onCommand(redisOp);

                if(future != null){
                    CommandReader<RedisOp> finalCmdReader = cmdReader;
                    future.addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            finalCmdReader.flushed(redisOp);
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
