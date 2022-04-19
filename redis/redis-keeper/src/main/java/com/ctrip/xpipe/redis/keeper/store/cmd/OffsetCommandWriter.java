package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.store.CommandFile;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandWriter;
import com.ctrip.xpipe.redis.core.store.CommandFileContext;
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

    private AtomicReference<CommandFileContext> cmdFileCtxRef;

    private CommandStore cmdStore;

    private int maxFileSize;

    private Logger delayTraceLogger;

    private static final Logger logger = LoggerFactory.getLogger(OffsetCommandWriter.class);

    public OffsetCommandWriter(CommandFileContext cmdFileContext, CommandStore cmdStore, int maxFileSize,
                               Logger delayTraceLogger) {
        this.cmdFileCtxRef = new AtomicReference<>(cmdFileContext);
        this.cmdStore = cmdStore;
        this.maxFileSize = maxFileSize;
        this.delayTraceLogger = delayTraceLogger;
    }

    @Override
    public void rotateFileIfNecessary() throws IOException {
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
            }
        }
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
}
