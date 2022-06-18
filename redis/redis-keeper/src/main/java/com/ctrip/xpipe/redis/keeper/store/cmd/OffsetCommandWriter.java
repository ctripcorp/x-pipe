package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.store.CommandFile;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandWriter;
import com.ctrip.xpipe.redis.core.store.CommandFileContext;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author lishanglin
 * date 2022/4/15
 */
public class OffsetCommandWriter implements CommandWriter {

    private AtomicReference<CommandFileContext> cmdFileCtxRef = new AtomicReference<>();

    private CommandStore cmdStore;

    private int maxFileSize;

    private Logger delayTraceLogger;

    private static final Logger logger = LoggerFactory.getLogger(OffsetCommandWriter.class);

    public OffsetCommandWriter(CommandStore cmdStore, int maxFileSize,
                               Logger delayTraceLogger) {
        this.cmdStore = cmdStore;
        this.maxFileSize = maxFileSize;
        this.delayTraceLogger = delayTraceLogger;
    }

    @Override
    public void initialize() throws IOException {
        if (null != cmdFileCtxRef.get()) return;

        CommandFile latestCommandFile = cmdStore.findLatestFile();
        CommandFileContext cmdFileCtx = new CommandFileContext(latestCommandFile);
        if (cmdFileCtxRef.compareAndSet(null, cmdFileCtx)) {
            logger.info("[initialize] write to {}", latestCommandFile.getFile().getName());
        }
    }

    @Override
    public boolean rotateFileIfNecessary() throws IOException {
        CommandFileContext curCmdFileCtx = cmdFileCtxRef.get();
        if (curCmdFileCtx.fileLength() >= maxFileSize) {
            CommandFile newCommandFile = cmdStore.newCommandFile(totalLength());
            File newFile = newCommandFile.getFile();
            long newStartOffset = newCommandFile.getStartOffset();
            logger.info("Rotate to {}", newFile.getName());

            synchronized (this) {
                CommandFileContext newCmdFileCtx = new CommandFileContext(newStartOffset, newFile);
                newCmdFileCtx.createIfNotExist();
                cmdFileCtxRef.set(newCmdFileCtx);

                curCmdFileCtx.close();
                return true;
            }
        }

        return false;
    }

    @Override
    public int write(ByteBuf byteBuf) throws IOException {
        if(delayTraceLogger.isDebugEnabled()) {
            delayTraceLogger.debug("[appendCommands][begin]{}");
        }

        CommandFileContext cmdFileCtx = cmdFileCtxRef.get();
        int wrote = ByteBufUtils.writeByteBufToFileChannel(byteBuf, cmdFileCtx.getChannel(), delayTraceLogger);

        if(delayTraceLogger.isDebugEnabled()){
            logger.debug("[appendCommands]{}, {}, {}", cmdFileCtx, byteBuf.readableBytes(), cmdFileCtx.fileLength());
        }

        long offset = cmdFileCtx.totalLength() - 1;
        if(delayTraceLogger.isDebugEnabled()){
            delayTraceLogger.debug("[appendCommands][ end ]{}", offset + 1);
        }

        return wrote;
    }

    @Override
    public synchronized long totalLength() {
        return cmdFileCtxRef.get().totalLength();
    }

    @Override
    public long getFileLastModified() {
        return cmdFileCtxRef.get().getLastModified();
    }

    @Override
    public void close() throws IOException {
        CommandFileContext cmdFileCtx = cmdFileCtxRef.get();
        if (null != cmdFileCtx) {
            cmdFileCtx.close();
        }
    }

    protected CommandStore getCommandStore() {
        return cmdStore;
    }

    protected Pair<CommandFile, Long> getWritePosition() throws IOException {
        CommandFileContext commandFileContext = cmdFileCtxRef.get();
        return Pair.of(commandFileContext.getCommandFile(), commandFileContext.getChannel().position());
    }

    protected int getMaxFileSize() {
        return maxFileSize;
    }

}
