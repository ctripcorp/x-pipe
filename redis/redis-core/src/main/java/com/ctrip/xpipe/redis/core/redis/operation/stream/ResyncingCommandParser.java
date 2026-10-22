package com.ctrip.xpipe.redis.core.redis.operation.stream;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.payload.DirectByteBufInOutPayload;
import com.ctrip.xpipe.payload.InOutPayloadFactory;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Command-stream parser that can re-align from an arbitrary offset (spec D19).
 * On syntax error it drops remnant, returns to SEEKING from the next byte after the
 * error position, and does not discard the rest of the buffer.
 * <p>
 * Only splits commands. Semantic checks (e.g. {@code PUBLISH}) belong to
 * {@code RedisOpItemParser} / {@code RedisOpType}.
 * ByteBuf retain/release is closed inside this parser; the command {@code ByteBuf}
 * given to the listener is valid only during {@link StreamCommandLister#onCommand}.
 */
public class ResyncingCommandParser {

    private static final Logger logger = LoggerFactory.getLogger(ResyncingCommandParser.class);

    public static final String MONITOR_TYPE = "ResyncingCommandParser";
    public static final String MONITOR_RESYNC = "resync";

    /**
     * ArrayParser uses {@code Integer.parse}-width sizes; more digits cannot be a real header.
     */
    private static final int MAX_ARRAY_LEN_DIGITS = 10;

    private enum State {
        SEEKING,
        PARSING
    }

    private enum HeaderMatch {
        COMPLETE,
        INCOMPLETE,
        FALSE
    }

    private final RedisClientProtocol<Object[]> protocolParser;
    private final StreamCommandLister commandLister;
    private final ByteBufAllocator allocator;

    private State state = State.SEEKING;
    private CompositeByteBuf remainingBuf;
    private ByteBuf seekRemain;

    public ResyncingCommandParser(StreamCommandLister commandLister) {
        this.protocolParser = new ArrayParser().setInOutPayloadFactory(new InOutPayloadFactory() {
            @Override
            public com.ctrip.xpipe.api.payload.InOutPayload create() {
                return new DirectByteBufInOutPayload();
            }
        });
        this.allocator = ByteBufAllocator.DEFAULT;
        this.commandLister = commandLister;
    }

    public void doRead(ByteBuf in) throws IOException {
        int prefixLen = 0;
        ByteBuf buf = in;
        if (seekRemain != null && seekRemain.isReadable()) {
            prefixLen = seekRemain.readableBytes();
            CompositeByteBuf composite = allocator.compositeBuffer(2);
            composite.addComponent(true, seekRemain);
            composite.addComponent(true, in.retainedSlice(in.readerIndex(), in.readableBytes()));
            seekRemain = null;
            buf = composite;
        }
        try {
            parseLoop(buf);
        } finally {
            finishRead(buf, in, prefixLen);
        }
    }

    public void reset() {
        releaseRemainingBuf();
        releaseSeekRemain();
        protocolParser.reset();
        state = State.SEEKING;
    }

    public int getRemainLength() {
        int remain = 0;
        if (remainingBuf != null) {
            remain += remainingBuf.readableBytes();
        }
        if (seekRemain != null) {
            remain += seekRemain.readableBytes();
        }
        return remain;
    }

    private void parseLoop(ByteBuf buf) throws IOException {
        while (buf.isReadable()) {
            if (state == State.SEEKING) {
                if (!seekHeader(buf)) {
                    return;
                }
                state = State.PARSING;
            }
            if (!parseOne(buf)) {
                return;
            }
        }
    }

    private boolean parseOne(ByteBuf buf) throws IOException {
        int pre = buf.readerIndex();
        RedisClientProtocol<Object[]> protocol;
        try {
            protocol = protocolParser.read(buf);
        } catch (Throwable t) {
            resync(buf, pre, t);
            return true;
        }
        if (protocol == null) {
            int consumed = buf.readerIndex() - pre;
            if (consumed > 0) {
                compositeUnCompleted(buf.retainedSlice(pre, consumed));
            }
            return false;
        }
        Object[] payload = protocol.getPayload();
        ByteBuf commandBuf = currentCommandBuf(buf.slice(pre, buf.readerIndex() - pre));
        try {
            commandLister.onCommand(payload, commandBuf);
        } finally {
            resetAfterCommand();
        }
        return true;
    }

    private void resync(ByteBuf buf, int pre, Throwable t) {
        logger.warn("[parseOne] resync", t);
        EventMonitor.DEFAULT.logEvent(MONITOR_TYPE, MONITOR_RESYNC);
        releaseRemainingBuf();
        protocolParser.reset();
        state = State.SEEKING;
        if (buf.readerIndex() == pre && buf.isReadable()) {
            buf.readByte();
        }
    }

    private boolean seekHeader(ByteBuf buf) {
        while (buf.isReadable()) {
            int star = indexOfAsterisk(buf);
            if (star < 0) {
                buf.skipBytes(buf.readableBytes());
                return false;
            }
            buf.readerIndex(star);
            HeaderMatch match = matchHeader(buf);
            if (match == HeaderMatch.COMPLETE) {
                return true;
            }
            if (match == HeaderMatch.INCOMPLETE) {
                return false;
            }
            buf.readerIndex(star + 1);
        }
        return false;
    }

    private static int indexOfAsterisk(ByteBuf buf) {
        int from = buf.readerIndex();
        int to = buf.writerIndex();
        for (int i = from; i < to; i++) {
            if (buf.getByte(i) == RedisClientProtocol.ASTERISK_BYTE) {
                return i;
            }
        }
        return -1;
    }

    private static HeaderMatch matchHeader(ByteBuf buf) {
        int star = buf.readerIndex();
        int end = buf.writerIndex();
        int i = star + 1;
        if (i >= end) {
            return HeaderMatch.INCOMPLETE;
        }
        int digits = 0;
        while (i < end) {
            byte b = buf.getByte(i);
            if (b >= '0' && b <= '9') {
                digits++;
                if (digits > MAX_ARRAY_LEN_DIGITS) {
                    return HeaderMatch.FALSE;
                }
                i++;
                continue;
            }
            break;
        }
        if (digits == 0) {
            return HeaderMatch.FALSE;
        }
        if (i >= end) {
            return HeaderMatch.INCOMPLETE;
        }
        if (buf.getByte(i) != '\r') {
            return HeaderMatch.FALSE;
        }
        i++;
        if (i >= end) {
            return HeaderMatch.INCOMPLETE;
        }
        if (buf.getByte(i) != '\n') {
            return HeaderMatch.FALSE;
        }
        return HeaderMatch.COMPLETE;
    }

    private ByteBuf currentCommandBuf(ByteBuf slice) {
        if (remainingBuf != null) {
            remainingBuf.addComponent(true, slice.retain());
            return remainingBuf;
        }
        return slice;
    }

    private void compositeUnCompleted(ByteBuf slice) {
        if (remainingBuf == null) {
            remainingBuf = allocator.compositeBuffer();
        }
        remainingBuf.addComponent(true, slice);
    }

    private void resetAfterCommand() {
        releaseRemainingBuf();
        protocolParser.reset();
    }

    private void finishRead(ByteBuf buf, ByteBuf in, int prefixLen) {
        if (buf == in) {
            stashSeekRemainIfNeeded(in);
            return;
        }
        try {
            int consumedFromIn = Math.max(0, buf.readerIndex() - prefixLen);
            int newInIndex = Math.min(in.writerIndex(), in.readerIndex() + consumedFromIn);
            in.readerIndex(newInIndex);
            if (state == State.SEEKING && buf.isReadable()) {
                seekRemain = Unpooled.copiedBuffer(buf.slice(buf.readerIndex(), buf.readableBytes()));
                in.readerIndex(in.writerIndex());
            }
        } finally {
            buf.release();
        }
    }

    private void stashSeekRemainIfNeeded(ByteBuf in) {
        if (state != State.SEEKING || !in.isReadable()) {
            return;
        }
        seekRemain = Unpooled.copiedBuffer(in);
        in.skipBytes(in.readableBytes());
    }

    private void releaseRemainingBuf() {
        if (remainingBuf != null) {
            remainingBuf.release();
            remainingBuf = null;
        }
    }

    private void releaseSeekRemain() {
        if (seekRemain != null) {
            seekRemain.release();
            seekRemain = null;
        }
    }
}
