package com.ctrip.xpipe.redis.core.protocal.protocal;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.payload.ByteArrayWritableByteChannel;
import com.ctrip.xpipe.payload.StringInOutPayload;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AbstractBulkStringParser extends BulkStringParser {
    public AbstractBulkStringParser(String content) {
        super(new StringInOutPayload(content));
    }

    public AbstractBulkStringParser(InOutPayload bulkStringPayload) {
        super(bulkStringPayload);
    }
    protected Logger logger = null;

    @Override
    protected ByteBuf getWriteByteBuf() {
        if(payload == null){
            if(logger.isInfoEnabled()){
                logger.info("[getWriteBytes][payload null]");
            }
            return Unpooled.wrappedBuffer(new byte[0]);
        }

        if((payload instanceof StringInOutPayload)|| (payload instanceof ByteArrayOutputStreamPayload)){
            try {
                ByteArrayWritableByteChannel channel = new ByteArrayWritableByteChannel();
                payload.out(channel);
                byte []content = channel.getResult();
                String length = String.valueOf((char)DOLLAR_BYTE) + content.length + RedisClientProtocol.CRLF;
                return Unpooled.wrappedBuffer(length.getBytes(), content, RedisClientProtocol.CRLF.getBytes());
            } catch (IOException e) {
                logger.error("[getWriteBytes]", e);
                return Unpooled.wrappedBuffer(new byte[0]);
            }
        }
        throw new UnsupportedOperationException();
    }

    @Override
    protected Logger getLogger() {
        if(logger == null) {
            logger = LoggerFactory.getLogger(getClass());
        }
        return logger;
    }
    protected BulkStringParser.BULK_STRING_STATE bulkStringState = BulkStringParser.BULK_STRING_STATE.READING_EOF_MARK;

    public enum BULK_STRING_STATE{
        READING_EOF_MARK,
        READING_CONTENT,
        READING_CR,
        READING_LF,
        END
    }

    protected BulkStringEofJudger eofJudger;

    private LfReader lfReader = null;
    protected BulkStringEofJudger readEOfMark(ByteBuf byteBuf){

        if(lfReader == null){
            lfReader = new LfReader();
        }
        RedisClientProtocol<byte[]> markBytes= lfReader.read(byteBuf);
        if(markBytes == null){
            return null;
        }

        if(markBytes.getPayload().length == 0){
            lfReader = null;
            return null;
        }

        return BulkStringEofJuderManager.create(markBytes.getPayload());
    }




    public void setEofJudger(BulkStringEofJudger eofJudger) {
        this.eofJudger = eofJudger;
        if (bulkStringParserListener != null) {
            bulkStringParserListener.onEofType(eofJudger.getEofType());
        }
    }



    public BulkStringEofJudger.JudgeResult addContext(ByteBuf byteBuf) {
        int readerIndex = byteBuf.readerIndex();
        BulkStringEofJudger.JudgeResult result = eofJudger.end(byteBuf.slice());
        int length = 0;
        try {
            length = payload.in(byteBuf.slice(readerIndex, result.getReadLen()));
            if (length != result.getReadLen()) {
                throw new IllegalStateException(String.format("expected readLen:%d, but real:%d", result.getReadLen(), length));
            }
        } catch (IOException e) {
            logger.error("[read][exception]" + payload, e);
            throw new RedisRuntimeException("[write to payload exception]" + payload, e);
        }
        byteBuf.readerIndex(readerIndex + length);
        return result;
    }

    public void startInput() {
        payload.startInput();
    }

    public void endInput() {
        int truncate = eofJudger.truncate();
        try {
            if (truncate > 0) {
                payload.endInputTruncate(truncate);
            } else {
                payload.endInput();
            }
        } catch (IOException e) {
            throw new RedisRuntimeException("[write to payload truncate exception]" + payload, e);
        }
    }

    protected  boolean cmpCharUpdateState(ByteBuf byteBuf, char eqbyte, BulkStringParser.BULK_STRING_STATE state) {
        if (byteBuf.readableBytes() == 0) {
            return false;
        }
        byte data1 = byteBuf.getByte(byteBuf.readerIndex());
        if (data1 == eqbyte) {
            byteBuf.readByte();
            bulkStringState = state;
            return true;
        } else {
            throw new RedisRuntimeException(String.format("Parse the Redis command protocol Error: Here should be '%s' ,but it's %s", eqbyte, (char)data1));
        }
    }



    @Override
    public boolean supportes(Class<?> clazz) {
        return InOutPayload.class.isAssignableFrom(clazz);
    }
}
