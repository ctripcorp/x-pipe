package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
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

    public GtidCommandStore(File file, int maxFileSize, IntSupplier maxTimeSecondKeeperCmdFileAfterModified,
                            int minTimeMilliToGcAfterModified, IntSupplier fileNumToKeep, long commandReaderFlyingThreshold,
                            CommandReaderWriterFactory cmdReaderWriterFactory,
                            KeeperMonitor keeperMonitor, RedisOpParser redisOpParser, ReplStage.ReplProto proto) throws IOException {
        super(file, maxFileSize, maxTimeSecondKeeperCmdFileAfterModified, minTimeMilliToGcAfterModified, fileNumToKeep,
                commandReaderFlyingThreshold, cmdReaderWriterFactory, keeperMonitor, redisOpParser, proto);
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

                final RedisOp redisOp = cmdReader.read(1000);

                if (null == redisOp) continue;

                logger.debug("[addCommandsListener] {}", redisOp);

                // TODO: monitor send delay
                ChannelFuture future = listener.onCommand(cmdReader.getCurCmdFile(), cmdReader.position(), redisOp);

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

    @Override
    public void setBaseIndex(String baseGtidSet, long localOffset) {
        //when fullSync or when keeperSync, rdbGtidSet come up later;
        //but when keeperRestart, rdbGtidSet come up immediately from meta.json

        logger.info("[setBaseIndex]{}-{}",localOffset, baseGtidSet);
        this.baseGtidSet = new GtidSet(baseGtidSet);
        this.baseStartOffset = localOffset;
    }

}
