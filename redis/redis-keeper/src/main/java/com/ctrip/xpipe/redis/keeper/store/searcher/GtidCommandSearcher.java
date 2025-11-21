package com.ctrip.xpipe.redis.keeper.store.searcher;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.payload.ByteArrayWritableByteChannel;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.store.CancelableCommandsGuarantee;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.FileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GtidCommandSearcher extends AbstractCommand<List<CmdKeyItem>> implements CommandsListener {

    private String uuid;

    private int begGno;

    private int endGno;

    private RedisKeeperServer redisKeeperServer;

    private RedisClientProtocol<Object[]> protocolParser;

    private RedisOpParser redisOpParser;

    private List<CmdKeyItem> cmdKeyItems;

    private static final Logger logger = LoggerFactory.getLogger(GtidCommandSearcher.class);

    public GtidCommandSearcher(String uuid, int begGno, int endGno, RedisKeeperServer redisKeeperServer, RedisOpParser redisOpParser) {
        this.uuid = uuid;
        this.begGno = begGno;
        this.endGno = endGno;
        this.redisKeeperServer = redisKeeperServer;
        this.protocolParser = new ArrayParser();
        this.redisOpParser = redisOpParser;
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
        return future().isDone();
    }

    @Override
    public ChannelFuture onCommand(CommandFile currentFile, long filePosition, Object cmd) {
        if (cmd instanceof FileRegion) {
            FileRegion fileRegion = (FileRegion) cmd;
            ByteArrayWritableByteChannel channel = new ByteArrayWritableByteChannel();
            try {
                // TODO: parse RedisOp from fileRegion
                fileRegion.transferTo(channel, 0);
                ByteBuf byteBuf = Unpooled.wrappedBuffer(channel.getResult());
                protocolParser.read(byteBuf);
            } catch (Exception e) {
                throw new XpipeRuntimeException("", e);
            }

        }

        return null;
    }

    @Override
    public void onCommandEnd() {

    }

    @Override
    public void beforeCommand() {

    }

    @Override
    public Long processedBacklogOffset() {
        return 0L;
    }

    @Override
    protected void doReset() {
        this.cmdKeyItems = new ArrayList<>();
    }

    @Override
    public String getName() {
        return "GtidCommandSearcher";
    }
}
