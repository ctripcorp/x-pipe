package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.api.proxy.ProxyRequestResponseProtocol;
import com.ctrip.xpipe.redis.core.exception.ProxyProtocolException;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.parser.CompositeProxyProtocolParser;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 22, 2018
 */
public class ProxyProtocolDecoder extends ByteToMessageDecoder {

    private static final Logger logger = LoggerFactory.getLogger(ProxyProtocolDecoder.class);

    private boolean finished = false, continuouslyDecode = false;

    private static final char[] PREFIX = new char[]{'+', 'P', 'R', 'O', 'X', 'Y'};

    private int maxLength, readLength = 0, bufReadIndex = 0;

    private ProxyProtocolParser parser = new CompositeProxyProtocolParser();

    public static final int DEFAULT_MAX_LENGTH = 1024;

    public ProxyProtocolDecoder(int maxLength) {
        this.maxLength = maxLength;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {

        checkValid(in);

        try {
            ProxyProtocol protocol = parser.read(in);
            if(protocol == null) {
                return;
            }
            out.add(protocol);
            // connection protocol, drop all protocol stuffs & build connection chain; otherwise, response for request
            if(protocol instanceof ProxyConnectProtocol) {
                finished = true;
                continuouslyDecode = false;
            } else {
                logger.debug("[{}][response-protocol] {}", ChannelUtil.getDesc(ctx.channel()),
                        ((ProxyRequestResponseProtocol)protocol).getContent());
                continuouslyDecode = true;
                reset();
            }
        } catch (Throwable t) {
            if(t instanceof ProxyProtocolException) {
                throw t;
            } else {
                throw new ProxyProtocolException("Proxy Protocol Analysis Error", t);
            }
        }

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
        if(finished) {
            ctx.pipeline().remove(this);
        }
    }

    @Override
    public boolean isSingleDecode() {
        return !continuouslyDecode;
    }

    private void checkValid(ByteBuf in) {
        if(readLength > maxLength) {
            throw new ProxyProtocolException("frame length (" + readLength + ") exceeds the allowed maximum ("
                    + maxLength + ')');
        }
        if(bufReadIndex < PREFIX.length && !matchProtocolFormat(in)) {
            String insideMessage = ByteBufUtil.prettyHexDump(in, in.readerIndex(), Math.min(PREFIX.length, in.readableBytes()));
            logger.debug("[checkValid] receive: idx:{}, {}", bufReadIndex, insideMessage);
            throw new ProxyProtocolException("Format error: " + insideMessage);
        }
        readLength += in.readableBytes();
    }

    private boolean matchProtocolFormat(ByteBuf in) {
        int index = in.readerIndex();
        int totalReadableBytes = in.readableBytes();
        for(; bufReadIndex < PREFIX.length && index < totalReadableBytes; bufReadIndex++) {
            if(in.getByte(index) != PREFIX[bufReadIndex]) {
                logger.warn("not equal: {}, {}", Character.toChars(in.getByte(index)), PREFIX[bufReadIndex]);
                return false;
            }
            index ++;
        }
        return true;
    }

    @VisibleForTesting
    protected boolean isFinished() {
        return finished;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if(!(cause instanceof ProxyProtocolException)) {
            logger.error("[exceptionCaught][close channel]" + ChannelUtil.getDesc(ctx.channel()), cause);
            super.exceptionCaught(ctx, cause);
        }
        ctx.channel().close();

    }

    private void reset() {
        readLength = 0;
        bufReadIndex = 0;
        parser = new CompositeProxyProtocolParser();
    }
}
