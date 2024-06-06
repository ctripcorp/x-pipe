package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lishanglin
 * date 2022/6/4
 */
public class RdbResizeDbParser extends AbstractRdbParser<Pair<Integer, Integer>> implements RdbParser<Pair<Integer, Integer>> {

    private RdbParseContext context;

    private STATE state = STATE.READ_INIT;

    private RdbLength dbSize;

    private RdbLength expireSize;

    private RdbLength crdtDbSize;

    private static final Logger logger = LoggerFactory.getLogger(RdbResizeDbParser.class);

    public RdbResizeDbParser(RdbParseContext parseContext) {
        this.context = parseContext;
    }

    enum STATE {
        READ_INIT,
        READ_DB_SIZE,
        READ_EXPIRE_SIZE,
        READ_CRDT_DB_SIZE,
        READ_END
    }

    @Override
    public Pair<Integer, Integer> read(ByteBuf byteBuf) {

        while (!isFinish() && byteBuf.readableBytes() > 0) {

            switch (state) {
                case READ_INIT:
                    dbSize = null;
                    expireSize = null;
                    state = STATE.READ_DB_SIZE;
                    break;

                case READ_DB_SIZE:
                    dbSize = parseRdbLength(byteBuf);
                    if (null != dbSize) state = STATE.READ_EXPIRE_SIZE;
                    break;

                case READ_EXPIRE_SIZE:
                    expireSize = parseRdbLength(byteBuf);
                    if (null != expireSize) {
                        if (context.isCrdt()) {
                            state = STATE.READ_CRDT_DB_SIZE;
                        } else {
                            state = STATE.READ_END;
                        }
                    }
                    break;
                case READ_CRDT_DB_SIZE:
                    crdtDbSize = parseRdbLength(byteBuf);
                    if (null != crdtDbSize) {
                        state = STATE.READ_END;
                    }
                    break;

                case READ_END:
                default:
            }

        }

        if (isFinish()) return Pair.of(dbSize.getLenValue(), expireSize.getLenValue());
        return null;
    }

    @Override
    public boolean isFinish() {
        return STATE.READ_END.equals(state);
    }

    @Override
    public void reset() {
        super.reset();
        this.state = STATE.READ_INIT;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
