package com.ctrip.xpipe.redis.keeper.store.searcher;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.payload.BoundedWritableByteChannel;
import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.stream.StreamTransactionListener;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.store.CancelableCommandsGuarantee;
import com.ctrip.xpipe.redis.keeper.store.gtid.index.StreamCommandReader;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.FileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GtidCommandSearcher extends AbstractCommand<List<CmdKeyItem>> implements CommandsListener, StreamTransactionListener {

    private String uuid;

    private int begGno;

    private int endGno;

    private RedisKeeperServer redisKeeperServer;

    private StreamCommandReader reader;

    private RedisOpParser redisOpParser;

    private List<CmdKeyItem> cmdKeyItems;

    private static final Logger logger = LoggerFactory.getLogger(GtidCommandSearcher.class);

    private String currentUUID;

    private int currentGno;
    
    /**
     * Default buffer size for BoundedWritableByteChannel (128KB)
     */
    private static final int DEFAULT_BUFFER_SIZE = 128 * 1024;

    public GtidCommandSearcher(String uuid, int begGno, int endGno, RedisKeeperServer redisKeeperServer, RedisOpParser redisOpParser) {
        this.uuid = uuid;
        this.begGno = begGno;
        this.endGno = endGno;
        this.redisKeeperServer = redisKeeperServer;
        this.redisOpParser = redisOpParser;
        this.reader = new StreamCommandReader(this, 0);
    }

    @Override
    protected void doExecute() throws Throwable {
        ReplicationStore store = this.redisKeeperServer.getReplicationStore();
        CommandsGuarantee guarantee = null;
        cmdKeyItems = new ArrayList<>();
        try {
            List<BacklogOffsetReplicationProgress> segments = store.locateCmdSegment(uuid, begGno, endGno);
            if (segments.isEmpty()) {
                future().setSuccess(Collections.emptyList());
                return;
            }

            guarantee = new CancelableCommandsGuarantee(segments.getFirst().getProgress(), System.currentTimeMillis(), 1800 * 1000);
            if (!store.retainCommands(guarantee)) {
                logger.info("[retain offset][fail] {}", guarantee.getBacklogOffset());
                future().setFailure(new XpipeRuntimeException("retain commands failed"));
                return;
            }

            for (BacklogOffsetReplicationProgress segment : segments) {
                store.addCommandsListener(segment, this);
            }
            future().setSuccess(cmdKeyItems);
        } finally {
            if (null != guarantee) {
                guarantee.cancel();
            }
        }
    }

    @Override
    public boolean isOpen() {
        return !future().isDone();
    }

    @Override
    public ChannelFuture onCommand(CommandFile currentFile, long filePosition, Object cmd) {
        if (cmd instanceof FileRegion) {
            FileRegion fileRegion = (FileRegion) cmd;
            try {
                // Use BoundedWritableByteChannel to limit memory usage and process data in chunks
                BoundedWritableByteChannel channel = new BoundedWritableByteChannel(
                    DEFAULT_BUFFER_SIZE,
                    byteBuf -> {
                        try {
                            // Process buffered data incrementally
                            reader.doRead(byteBuf);
                        } catch (IOException e) {
                            throw new XpipeRuntimeException("Error processing buffered data", e);
                        }
                    }
                );
                
                try {
                    // Transfer file content to channel, which will process data in chunks
                    fileRegion.transferTo(channel, 0);
                    // Flush any remaining data in the buffer
                    channel.flush();
                } finally {
                    channel.close();
                }
            } catch (Exception e) {
                throw new XpipeRuntimeException("Error processing file region", e);
            }
        }

        return null;
    }

    @Override
    public void onCommandEnd() {
        reader.resetParser();
    }

    @Override
    public void beforeCommand() {
        // do nothing
    }

    @Override
    public Long processedBacklogOffset() {
        return null;
    }

    @Override
    public boolean preAppend(String gtid, long offset) throws IOException {
        if (null == gtid) {
            return false;
        }

        String[] raw = gtid.split(":");
        currentUUID = raw[0];
        currentGno = Integer.parseInt(raw[1]);
        if (!currentUUID.equalsIgnoreCase(uuid) || currentGno < begGno || currentGno > endGno) {
            return false;
        }

        return true;
    }

    private void afterAppend() {
        currentUUID = null;
        currentGno = -1;
    }

    @Override
    public int postAppend(ByteBuf commandBuf, Object[] payload) throws IOException {
        try {
            RedisOp redisOp = redisOpParser.parse(payload);
            appendCmdKeyItem(currentUUID, currentGno, redisOp);
        } catch (Throwable th) {
            logger.info("[postAppend][parse fail][{}:{}] skip", currentUUID, currentGno, th);
        } finally {
            afterAppend();
        }
        return 0;
    }

    @Override
    public int batchPostAppend(List<ByteBuf> commandBufs, List<Object[]> payloads) throws IOException {
        try {
            for (Object[] payload : payloads) {
                RedisOp redisOp = redisOpParser.parse(payload);
                appendCmdKeyItem(currentUUID, currentGno, redisOp);
            }
        } catch (Throwable th) {
            logger.info("[batchPostAppend][parse fail][{}:{}] skip", currentUUID, currentGno, th);
        } finally {
            afterAppend();
        }
        return 0;
    }

    private void appendCmdKeyItem(String uuid, int gno, RedisOp redisOp) {
        if (StringUtil.isEmpty(uuid) || gno <= 0) {
            logger.debug("[appendCmdKeyItem][miss gtid] {}:{}", uuid, gno);
            return;
        }

        int dbId = 0;
        try {
            dbId = Integer.parseInt(redisOp.getDbId());
        } catch (NumberFormatException e) {
            logger.info("[appendCmdKeyItem][invalid dbId][{}:{}] {}", uuid, gno, redisOp.getDbId());
        }
        if (redisOp instanceof RedisMultiKeyOp) {
            RedisMultiKeyOp redisMultiKeyOp = (RedisMultiKeyOp) redisOp;
            List<RedisKey> keys = redisMultiKeyOp.getKeys();
            for (RedisKey redisKey : keys) {
                if (null == redisKey) continue;
                CmdKeyItem item = new CmdKeyItem(uuid, gno, dbId, redisOp.getOpType().name(), redisKey.get());
                cmdKeyItems.add(item);
            }
        } else if (redisOp instanceof RedisSingleKeyOp) {
            RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
            RedisKey redisKey = redisSingleKeyOp.getKey();
            if (null == redisKey) return;
            CmdKeyItem item = new CmdKeyItem(uuid, gno, dbId, redisOp.getOpType().name(), redisKey.get());
            cmdKeyItems.add(item);
        } else if (redisOp instanceof RedisMultiSubKeyOp) {
            RedisMultiSubKeyOp redisSubKeyOp = (RedisMultiSubKeyOp) redisOp;
            RedisKey redisKey = redisSubKeyOp.getKey();
            if (null == redisKey) return;
            for (RedisKey subKey: redisSubKeyOp.getAllSubKeys()) {
                CmdKeyItem item = new CmdKeyItem(uuid, gno, dbId, redisOp.getOpType().name(), redisKey.get(), subKey.get());
                cmdKeyItems.add(item);
            }
        }
    }

    @VisibleForTesting
    protected void setCmdKeyItems(List<CmdKeyItem> cmdKeyItems) {
        this.cmdKeyItems = cmdKeyItems;
    }

    @VisibleForTesting
    protected List<CmdKeyItem> getCmdKeyItems() {
        return cmdKeyItems;
    }

    @Override
    public boolean checkOffset(long offset) {
        return true;
    }

    @Override
    protected void doReset() {
        this.cmdKeyItems = new ArrayList<>();
        this.reader.resetParser();
    }

    @Override
    public String getName() {
        return "GtidCommandSearcher";
    }
}
