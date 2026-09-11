package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.exception.RdbRejectedException;
import com.ctrip.xpipe.redis.core.protocal.protocal.RdbBulkStringParser;
import io.netty.buffer.ByteBuf;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Client for {@code PSYNC ? -4} (keeper cmd-tail, no index / no GTID).
 * <p>
 * {@code +CONTINUE <replId> <offset>} is dispatched by the parent as
 * {@link #doOnKeeperContinue(String, long)} → {@code onKeeperContinue(replId, offset)}.
 * That offset is the <b>first byte to send</b>; consumers must use it as-is and must
 * not {@code +1}. {@code ? -4} never yields {@code XCONTINUE … REPLOFF}, so there is
 * no ±1 fork. Bytes are raw cmd stream (XSYNC stage may contain GTID commands);
 * this class does no GTID processing.
 * <p>
 * RDB is rejected in-class: {@link #createRdbReader()}, {@link #failReadRdb(Throwable)},
 * {@link #doOnFullSync()}, {@link #doOnXFullSync()} all throw {@link RdbRejectedException}
 * and mark this command closed. Comparator must detect this by type, not by exception
 * message. Closing the TCP channel is the owner's job (a {@code FixedObjectPool}
 * wrapping the replica connection, same pattern as
 * {@code AbstractRedisMasterReplication}). This class does not override
 * {@code afterCommandExecute} and does not send {@code REPLCONF}.
 * <p>
 * Parent {@link AbstractGapAllowedSync#doReset()} throws {@link UnsupportedOperationException}.
 * Reconnect must {@code new} another instance; do not reset / reuse.
 * This class holds no Store.
 */
public class CmdTailGapAllowedSync extends AbstractGapAllowedSync {

    private final CmdTailStreamListener listener;

    public CmdTailGapAllowedSync(SimpleObjectPool<NettyClient> clientPool,
                                 CmdTailStreamListener listener,
                                 ScheduledExecutorService scheduled) {
        super(clientPool, true, scheduled);
        this.listener = Objects.requireNonNull(listener, "listener");
    }

    @Override
    public String getName() {
        return "cmd-tail-gasync";
    }

    @Override
    public SyncRequest getSyncRequest() {
        PsyncRequest request = new PsyncRequest();
        request.setReplId("?");
        request.setReplOff(KEEPER_CMD_TAIL_SYNC_OFFSET);
        return request;
    }

    @Override
    protected void appendCommands(ByteBuf byteBuf) {
        listener.onCommands(byteBuf);
    }

    @Override
    protected RdbBulkStringParser createRdbReader() {
        throw rejectRdb("createRdbReader");
    }

    @Override
    protected void failReadRdb(Throwable throwable) {
        throw rejectRdb("failReadRdb", throwable);
    }

    @Override
    protected void doOnFullSync() {
        throw rejectRdb("doOnFullSync");
    }

    @Override
    protected void doOnXFullSync() {
        throw rejectRdb("doOnXFullSync");
    }

    private RdbRejectedException rejectRdb(String method) {
        return rejectRdb(method, null);
    }

    private RdbRejectedException rejectRdb(String method, Throwable cause) {
        close();
        if (cause == null) {
            return new RdbRejectedException(method);
        }
        return new RdbRejectedException(method, cause);
    }
}
