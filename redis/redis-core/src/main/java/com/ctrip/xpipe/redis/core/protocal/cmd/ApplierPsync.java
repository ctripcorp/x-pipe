package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.Sync;
import com.ctrip.xpipe.redis.core.protocal.protocal.AbstractBulkStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.ctrip.xpipe.redis.core.protocal.Sync.SYNC_STATE.*;


/**
 * @author hailu
 * @date 2024/4/29 19:34
 */
public class ApplierPsync extends AbstractSync implements Sync, AbstractBulkStringParser.BulkStringParserListener, InOutPayload {
    private AtomicLong offsetRecorder;

    private AtomicReference<String> replId;

    public ApplierPsync(String host, int port, AtomicReference<String> replId, AtomicLong offsetRecorder, ScheduledExecutorService scheduled) {
        super(host, port, scheduled);
        this.replId = replId;
        this.offsetRecorder = offsetRecorder;
    }

    public ApplierPsync(SimpleObjectPool<NettyClient> clientPool, AtomicReference<String> replId, AtomicLong offsetRecorder, ScheduledExecutorService scheduled, int listeningPort) {
        super(clientPool, scheduled);
        this.replId = replId;
        this.offsetRecorder = offsetRecorder;
        this.listeningPort = listeningPort;
        this.currentCommandOffset = new AtomicLong(0);
    }

    @Override
    // psync runID offset
    public ByteBuf getRequest() {
        long offsetRequest = "?".equalsIgnoreCase(replId.get()) ? -1 : offsetRecorder.get() + 1;
        RequestStringParser requestString = new RequestStringParser(getName(), replId.get(),
                String.valueOf(offsetRequest));
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("[doRequest]{}, {}", this, StringUtil.join(" ", requestString.getPayload()));
        }
        return requestString.format();
    }

    //
    // PARTIAL: +CONTINUE
    protected void handleRedisResponse(Channel channel, String psync) throws IOException {
        if (getLogger().isInfoEnabled()) {
            getLogger().info("[handleRedisResponse]{}, {}", ChannelUtil.getDesc(channel), psync);
        }

        String[] split = splitSpace(psync);
        if (split.length == 0) {
            throw new RedisRuntimeException("wrong reply:" + psync);
        }

        if (split[0].equalsIgnoreCase(FULL_SYNC)) {
            if (split.length != 3) {
                throw new RedisRuntimeException("unknown reply:" + psync);
            }
            String newReplId = split[1];
            if (!replId.get().equals("?") && !replId.get().equals(newReplId)) {
                getLogger().info("[handleRedisResponse][replId changed]{}, {}, {}, {}", ChannelUtil.getDesc(channel), this, replId, newReplId);
            }
            replId.set(newReplId);
            rdbOffset = Long.parseLong(split[2]);
            // reset offset recorder
            offsetRecorder.set(-1);
            getLogger().debug("[readRedisResponse]{}, {}, {}, {}", ChannelUtil.getDesc(channel), this, replId, rdbOffset);
            syncState = READING_RDB;
            doOnFullSync();
        } else if (split[0].equalsIgnoreCase(PARTIAL_SYNC)) {
            syncState = READING_COMMANDS;
            getLogger().debug("[readRedisResponse][PARTIAL]{}, {}", ChannelUtil.getDesc(channel), this);
            long continueOffset = offsetRecorder.get();
            doOnContinue(continueOffset);
        } else {
            throw new RedisRuntimeException("unknown reply:" + psync);
        }
    }

    @Override
    public String getName() {
        return Sync.PSYNC;
    }
}
