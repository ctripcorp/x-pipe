package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.gtid.GtidSet;
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

import static com.ctrip.xpipe.redis.core.protocal.Sync.SYNC_STATE.READING_COMMANDS;

/**
 * @author lishanglin
 * date 2022/2/23
 */
public class DefaultXsync extends AbstractSync implements Sync, AbstractBulkStringParser.BulkStringParserListener, InOutPayload {

    private Object vectorClockExcluded;

    public DefaultXsync(String host, int port, GtidSet gtidSetExcluded, Object vectorClockExcluded, ScheduledExecutorService scheduled) {
        super(host, port, scheduled);
        this.gitdSetExcluded = gtidSetExcluded;
        this.vectorClockExcluded = vectorClockExcluded;
    }

    public DefaultXsync(SimpleObjectPool<NettyClient> clientPool, GtidSet gtidSetExcluded, Object vectorClockExcluded, ScheduledExecutorService scheduled, int listeningPort) {
        super(clientPool, scheduled);
        this.gitdSetExcluded = gtidSetExcluded;
        this.vectorClockExcluded = vectorClockExcluded;
        this.listeningPort = listeningPort;
        this.currentCommandOffset = new AtomicLong(0);
    }

    @Override
    // XSYNC <sidno interested> <gtid.set excluded> [vectorclock excluded]
    public ByteBuf getRequest() {
        String interestedSidno = String.join(SIDNO_SEPARATOR, this.gitdSetExcluded.getUUIDs());

        RequestStringParser requestString = new RequestStringParser(getName(), interestedSidno,
                gitdSetExcluded.toString(), null != vectorClockExcluded ? vectorClockExcluded.toString() : "");
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("[doRequest]{}, {}", this, StringUtil.join(" ", requestString.getPayload()));
        }
        return requestString.format();
    }

    // FULL: +FULLRESYNC <RDB gtid.set>
    // PARTIAL: +CONTINUE
    protected void handleRedisResponse(Channel channel, String xsync) throws IOException {
        if (getLogger().isInfoEnabled()) {
            getLogger().info("[handleRedisResponse]{}, {}", ChannelUtil.getDesc(channel), xsync);
        }

        String[] split = splitSpace(xsync);
        if (split.length == 0) {
            throw new RedisRuntimeException("wrong reply:" + xsync);
        }

        if (split[0].equalsIgnoreCase(FULL_SYNC)) {
            if (split.length < 2) {
                throw new RedisRuntimeException("unknown reply:" + xsync);
            }
            this.rdbDataGtidSet = GtidSet.PLACE_HOLDER.equals(split[1]) ? new GtidSet(GtidSet.EMPTY_GTIDSET) : new GtidSet(split[1]);
            if (split.length > 2) {
                this.rdbOffset = Long.parseLong(split[2]);
            }
            getLogger().debug("[readRedisResponse][FULL]{}, {} {}", ChannelUtil.getDesc(channel), this, rdbDataGtidSet);
            syncState = SYNC_STATE.READING_RDB;
            doOnFullSync();
        } else if (split[0].equalsIgnoreCase(PARTIAL_SYNC)) {
            syncState = READING_COMMANDS;
            getLogger().debug("[readRedisResponse][PARTIAL]{}, {}", ChannelUtil.getDesc(channel), this);
            long continueOffset = 0;
            if (split.length > 1) {
                continueOffset = Long.parseLong(split[1]);
            }
            doOnContinue(continueOffset);
        } else {
            throw new RedisRuntimeException("unknown reply:" + xsync);
        }
    }

    @Override
    public String getName() {
        return Sync.XSYNC;
    }
}
