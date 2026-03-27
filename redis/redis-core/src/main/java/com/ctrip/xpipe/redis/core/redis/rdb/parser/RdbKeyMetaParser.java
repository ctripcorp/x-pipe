package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;

/**
 * @author TB
 * @date 2026/3/16 14:59
 */
public class RdbKeyMetaParser extends AbstractRdbParser<Void> implements RdbParser<Void> {
    public RdbKeyMetaParser(RdbParseContext context) {
    }

    private enum State { READ_NUM_CLASSES, READ_ATTR_TYPE, READ_ATTR_VALUE, READ_FINISH }
    private State state = State.READ_NUM_CLASSES;
    private long numClasses;
    private int attrsRead = 0;
    private KeyMeta keyMeta = new KeyMeta();
    private int currentAttrType = 0;

    public KeyMeta getKeyMeta() { return keyMeta; }

    public class KeyMeta {
        private long expireMs = -1;
        private long lruIdle = -1;
        private int lfuFreq = -1;
        // getters/setters

        public long getExpireMs() {
            return expireMs;
        }

        public void setExpireMs(long expireMs) {
            this.expireMs = expireMs;
        }

        public long getLruIdle() {
            return lruIdle;
        }

        public void setLruIdle(long lruIdle) {
            this.lruIdle = lruIdle;
        }

        public int getLfuFreq() {
            return lfuFreq;
        }

        public void setLfuFreq(int lfuFreq) {
            this.lfuFreq = lfuFreq;
        }
    }

    @Override
    public Void read(ByteBuf byteBuf) {
        while (!isFinish() && byteBuf.readableBytes() > 0) {
            switch (state) {
                case READ_NUM_CLASSES:
                    RdbLength len = parseRdbLength(byteBuf);
                    if (len != null) {
                        numClasses = len.getLenValue();
                        if (numClasses == 0) {
                            state = State.READ_FINISH;
                        } else {
                            state = State.READ_ATTR_TYPE;
                        }
                    }
                    break;
                case READ_ATTR_TYPE:
                    RdbLength attrLen = parseRdbLength(byteBuf);
                    if (attrLen != null) {
                        int attrType = (int) attrLen.getLenValue();
                        state = State.READ_ATTR_VALUE;
                        // 暂存属性类型，由下一个状态读取值
                        currentAttrType = attrType;
                    }
                    break;
                case READ_ATTR_VALUE:
                    switch (currentAttrType) {
                        case RdbConstant.KEY_META_ID_EXPIRE:
                            long expire = readMillisecondTime(byteBuf);
                            keyMeta.setExpireMs(expire);
                            break;
                        case RdbConstant.KEY_META_ID_LRU:
                            RdbLength lruLen = parseRdbLength(byteBuf);
                            if (lruLen != null) {
                                keyMeta.setLruIdle(lruLen.getLenValue());
                            } else return null;
                            break;
                        case RdbConstant.KEY_META_ID_LFU:
                            if (byteBuf.readableBytes() < 1) return null;
                            keyMeta.setLfuFreq(byteBuf.readByte() & 0xFF);
                            break;
                        default:
                            // 未知属性，尝试根据长度跳过？这里简化，抛异常或忽略
                            throw new XpipeRuntimeException("Unknown key meta attr: " + currentAttrType);
                    }
                    attrsRead++;
                    if (attrsRead >= numClasses) {
                        state = State.READ_FINISH;
                    } else {
                        state = State.READ_ATTR_TYPE;
                    }
                    break;
                case READ_FINISH:
                    isFinish();
                    break;
            }
        }
        return null;
    }

    @Override
    protected Logger getLogger() {
        return null;
    }

    @Override
    public boolean isFinish() {
        return false;
    }

    private long readMillisecondTime(ByteBuf byteBuf){
        return 0;
    }

}
