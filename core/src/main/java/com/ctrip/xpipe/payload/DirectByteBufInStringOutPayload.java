package com.ctrip.xpipe.payload;

import com.ctrip.xpipe.api.codec.Codec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public class DirectByteBufInStringOutPayload extends AbstractInOutPayload {

    private static final int INIT_SIZE = 1 << 8; //256

    private ByteBuf cumulation;

    private final Cumulator cumulator = COMPOSITE_CUMULATOR;

    private String result;

    @Override
    protected void doStartInput() {
        super.doStartInput();
        cumulation = Unpooled.compositeBuffer(INIT_SIZE);
    }

    @Override
    protected int doIn(ByteBuf byteBuf) throws IOException {
        cumulation = cumulator.cumulate(cumulation, byteBuf);
        byteBuf.retain();
        return byteBuf.readableBytes();
    }

    @Override
    protected void doTruncate(int reduceLen) throws IOException {
        cumulation.writerIndex(cumulation.writerIndex() -  reduceLen);
    }

    @Override
    protected long doOut(WritableByteChannel writableByteChannel) throws IOException {
        throw new UnsupportedOperationException("Not support");
    }

    @Override
    protected void doEndInput() throws IOException {
        result = cumulation.toString(Codec.defaultCharset);
        if(cumulation instanceof CompositeByteBuf) {
            ((CompositeByteBuf) cumulation).removeComponents(0, ((CompositeByteBuf) cumulation).numComponents());
        }
        cumulation.release();
        super.doEndInput();
    }

    @Override
    public String toString() {
        return result;
    }

    /**
     * Cumulate {@link ByteBuf}s by add them to a {@link CompositeByteBuf} and so do no memory copy whenever possible.
     * Be aware that {@link CompositeByteBuf} use a more complex indexing implementation so depending on your use-case
     * and the decoder implementation this may be slower then just use the {}.
     */
    public static final Cumulator COMPOSITE_CUMULATOR = new Cumulator() {

        @Override
        public ByteBuf cumulate(ByteBuf cumulation, ByteBuf in) {
            ByteBuf buffer;
            if (cumulation.refCnt() > 1) {
                // Expand cumulation (by replace it) when the refCnt is greater then 1 which may happen when the user
                // use slice().retain() or duplicate().retain().
                //
                // See:
                // - https://github.com/netty/netty/issues/2327
                // - https://github.com/netty/netty/issues/1764
                buffer = expandCumulation(cumulation, in.readableBytes());
                buffer.writeBytes(in);
                in.release();
            } else {
                CompositeByteBuf composite;
                if (cumulation instanceof CompositeByteBuf) {
                    composite = (CompositeByteBuf) cumulation;
                } else {
                    composite = Unpooled.compositeBuffer(INIT_SIZE);
                    composite.addComponent(true, cumulation);
                }
                composite.addComponent(true, in);
                buffer = composite;
            }
            return buffer;
        }
    };

    public static final Cumulator MERGE_CUMULATOR = new Cumulator() {
        @Override
        public ByteBuf cumulate(ByteBuf cumulation, ByteBuf in) {
            final ByteBuf buffer;
            if (cumulation.writerIndex() > cumulation.maxCapacity() - in.readableBytes()
                    || cumulation.refCnt() > 1 || cumulation.isReadOnly()) {
                // Expand cumulation (by replace it) when either there is not more room in the buffer
                // or if the refCnt is greater then 1 which may happen when the user use slice().retain() or
                // duplicate().retain() or if its read-only.
                //
                // See:
                // - https://github.com/netty/netty/issues/2327
                // - https://github.com/netty/netty/issues/1764
                buffer = expandCumulation(cumulation, in.readableBytes());
            } else {
                buffer = cumulation;
            }
            buffer.writeBytes(in);
            in.release();
            return buffer;
        }
    };

    private static ByteBuf expandCumulation(ByteBuf cumulation, int readable) {
        ByteBuf oldCumulation = cumulation;
        cumulation = Unpooled.buffer(oldCumulation.readableBytes() + readable);
        cumulation.writeBytes(oldCumulation);
        oldCumulation.release();
        return cumulation;
    }

    private interface Cumulator {
        ByteBuf cumulate(ByteBuf cumulation, ByteBuf in);
    }
}
