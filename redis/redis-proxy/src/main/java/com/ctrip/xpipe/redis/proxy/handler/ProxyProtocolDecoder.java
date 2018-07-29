package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.core.exception.ProxyProtocolException;
import com.ctrip.xpipe.redis.core.proxy.DefaultProxyProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocolParser;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 22, 2018
 */
public class ProxyProtocolDecoder extends ByteToMessageDecoder {

    private static final Logger logger = LoggerFactory.getLogger(ProxyProtocolDecoder.class);

    private boolean finished = false;

    private static final char[] PREFIX = new char[]{'+', 'P', 'R', 'O', 'X', 'Y'};

    private int maxLength, readLength = 0, bufReadIndex = 0;

    private ProxyProtocolParser parser = new DefaultProxyProtocolParser();

    public static final int DEFAULT_MAX_LENGTH = 2048;

    public ProxyProtocolDecoder(int maxLength) {
        this.maxLength = maxLength;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

        checkValid(in);

        try {
            ProxyProtocol proxyProtocol = parser.read(in);
            if(proxyProtocol == null) {
                return;
            }
            if(ctx.channel().remoteAddress() instanceof InetSocketAddress) {
                proxyProtocol.recordForwardFor((InetSocketAddress) ctx.channel().remoteAddress());
            }
            out.add(proxyProtocol);
            finished = true;
        } catch (ProxyProtocolException e) {
            throw e;
        } catch (Throwable t) {
            throw new ProxyProtocolException("Proxy Protocol Analysis Error", t);
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
        return true;
    }

    private void checkValid(ByteBuf in) {
        if(bufReadIndex < PREFIX.length && !matchProtocolFormat(in)) {
            String insideMessage = ByteBufUtil.prettyHexDump(in);
            in.release();
            logger.error("[checkValid] receive: {}", insideMessage);
            throw new ProxyProtocolException("Format error: " + insideMessage);
        }
        readLength += in.readableBytes();
        if(readLength > maxLength) {
            throw new ProxyProtocolException("frame length (" + readLength + ") exceeds the allowed maximum ("
                    + maxLength + ')');
        }
    }

    private boolean matchProtocolFormat(ByteBuf in) {
        for(; bufReadIndex < PREFIX.length && bufReadIndex < in.readableBytes(); bufReadIndex++) {
            if(in.getByte(bufReadIndex) != PREFIX[bufReadIndex]) {
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    protected boolean isFinished() {
        return finished;
    }
}
