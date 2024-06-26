package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.core.redis.operation.ieee754.BitUtils;
import com.ctrip.xpipe.redis.core.redis.operation.ieee754.IEEE754;
import com.ctrip.xpipe.redis.core.redis.operation.ieee754.IEEE754Format;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author hailu
 * @date 2024/6/7 14:02
 */
public abstract class AbstractRdbCrdtParser<T> extends AbstractRdbParser<T> implements RdbParser<T> {
    private Logger logger = LoggerFactory.getLogger(AbstractRdbCrdtParser.class);

    protected static final int LWW_TYPE = 0;
    protected static final int ORSET_TYPE = 1;

    protected static final int VALUE_TYPE_NONE = 0;
    protected static final int VALUE_TYPE_LONGLONG = 1;
    protected static final int VALUE_TYPE_DOUBLE = 2;
    protected static final int VALUE_TYPE_SDS = 3;
    protected static final int VALUE_TYPE_LONGDOUBLE = 4;

    protected static final int TAG_B = 1;
    protected static final int TAG_A = 2;
    protected static final int TAG_D = 4;

    protected static final String END_MARK = "end";

    CRDT_READ_STATE readState = CRDT_READ_STATE.READ_INIT;
    REGISTER_STATE registerState = REGISTER_STATE.READ_INIT;

    RC_STATE rcState = RC_STATE.READ_INIT;
    READ_VC_STATE vcState = READ_VC_STATE.READ_INIT;

    CRDT_READ_LIST_STATE readListState = CRDT_READ_LIST_STATE.READ_LIST_INIT;

    protected RdbParseContext context;

    private RdbLength opCode;

    private List<RdbLength> valueList;

    private RdbLength header;
    private int version;
    private RdbLength vcLength;

    private RdbLength gid;
    private RdbLength timestamp;
    private RdbLength valueLength;
    private RdbLength currentOperateType;
    private RdbLength counterType;
    private RdbLength currentDataType;
    private RdbLength valueType;
    private int readValueCount;
    private byte[] val;
    private double rcValue;
    private byte[] rcString;

    enum CRDT_READ_STATE {
        READ_INIT,
        READ_TYPE,
        READ_VALUE
    }

    enum READ_VC_STATE {
        READ_INIT,
        READ_STR_VC,
        READ_VC_LENGTH,
        READ_VC_VALUE
    }

    enum CRDT_READ_LIST_STATE {
        READ_LIST_INIT,
        READ_LIST_TYPE,
        READ_LIST_VALUE
    }

    enum REGISTER_STATE {
        READ_INIT,
        READ_HEAD,
        READ_GID,
        READ_TIMESTAMP,
        READ_VECTOR_CLOCK,
        READ_VAL
    }

    enum RC_STATE {
        READ_INIT,
        READ_LENGTH,
        READ_GID,
        READ_OPERATE_TYPE,
        READ_TYPE,
        READ_COUNTER_TYPE,
        READ_DATA_BASE,
        READ_DATA_ADD,
        READ_DATA_DECR
    }

    enum READ_VALUE_STATE {
        READ_VALUE_INIT,
        READ_VALUE_TYPE,
        READ_VALUE
    }

    protected RdbLength parseSigned(ByteBuf byteBuf) {
        while (byteBuf.readableBytes() > 0) {
            switch (readState) {
                case READ_INIT:
                    opCode = null;
                    readState = CRDT_READ_STATE.READ_TYPE;
                    break;

                case READ_TYPE:
                    opCode = parseRdbLength(byteBuf);
                    if (null != opCode) {
                        readState = CRDT_READ_STATE.READ_VALUE;
                    }
                    break;

                case READ_VALUE:
                    RdbLength value = parseRdbLength(byteBuf);
                    if (null != value) {
                        readState = CRDT_READ_STATE.READ_INIT;
                        opCode = null;
                        return value;
                    }
            }
        }
        return null;
    }

    protected byte[] parseString(ByteBuf byteBuf) {
        while (byteBuf.readableBytes() > 0) {
            switch (readState) {
                case READ_INIT:
                    opCode = null;
                    readState = CRDT_READ_STATE.READ_TYPE;
                    break;

                case READ_TYPE:
                    opCode = parseRdbLength(byteBuf);
                    if (null != opCode) {
                        readState = CRDT_READ_STATE.READ_VALUE;
                    }
                    break;
                case READ_VALUE:
                    RdbParser<byte[]> subKeyParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
                    byte[] key = subKeyParser.read(byteBuf);
                    if (null != key) {
                        readState = CRDT_READ_STATE.READ_INIT;
                        subKeyParser.reset();
                        return key;
                    }
            }
        }
        return null;
    }

    protected byte[] parseRegister(ByteBuf byteBuf) {

        while (byteBuf.readableBytes() > 0) {

            switch (registerState) {
                case READ_INIT:
                    header = null;
                    gid = null;
                    timestamp = null;
                    registerState = REGISTER_STATE.READ_HEAD;
                    vcLength = null;
                    break;
                case READ_HEAD:
                    header = parseSigned(byteBuf);
                    if (header == null) {
                        break;
                    }
                    int type = (int) header.getLenLongValue() & ((1 << 8) - 1);
                    version = ((int) (header.getLenLongValue() >> 48) & ((1 << 16) - 1));
                    if (type == LWW_TYPE) {
                        registerState = REGISTER_STATE.READ_GID;
                    } else {
                        reset();
                        return END_MARK.getBytes();
                    }
                    break;
                case READ_GID:
                    gid = parseSigned(byteBuf);
                    if (gid != null) {
                        registerState = REGISTER_STATE.READ_TIMESTAMP;
                    }
                    break;

                case READ_TIMESTAMP:
                    timestamp = parseSigned(byteBuf);
                    if (timestamp != null) {
                        registerState = REGISTER_STATE.READ_VECTOR_CLOCK;
                    }
                    break;

                case READ_VECTOR_CLOCK:
                    byte[] vc = parseVectorClock(byteBuf, version);
                    if (vc != null) {
                        registerState = REGISTER_STATE.READ_VAL;
                    }
                    break;

                case READ_VAL:
                    if (context.getCrdtType().isTombstone()) {
                        registerState = REGISTER_STATE.READ_INIT;
                        return END_MARK.getBytes();
                    }
                    byte[] value = parseString(byteBuf);
                    if (value != null) {
                        registerState = REGISTER_STATE.READ_INIT;
                        return value;
                    }
            }
        }
        return null;
    }

    protected byte[] parseRc(ByteBuf byteBuf) {

        while (byteBuf.readableBytes() > 0) {

            switch (rcState) {
                case READ_INIT:
                    gid = null;
                    val = null;
                    rcValue = 0.0;
                    rcString = null;
                    valueLength = null;
                    currentOperateType = null;
                    currentDataType = null;
                    counterType = null;
                    readValueCount = 0;
                    rcState = RC_STATE.READ_LENGTH;
                    break;

                case READ_LENGTH:
                    valueLength = parseSigned(byteBuf);
                    if (valueLength != null) {
                        if (valueLength.getLenValue() <= 0) {
                            rcState = RC_STATE.READ_INIT;
                            return END_MARK.getBytes();
                        } else {
                            rcState = RC_STATE.READ_GID;
                        }
                    }
                    break;
                case READ_GID:
                    gid = parseSigned(byteBuf);
                    if (gid != null) {
                        rcState = RC_STATE.READ_OPERATE_TYPE;
                    }
                    break;
                case READ_OPERATE_TYPE:
                    currentOperateType = parseSigned(byteBuf);
                    if (currentOperateType != null) {
                        rcState = RC_STATE.READ_TYPE;
                    }
                    break;
                case READ_TYPE:
                    currentDataType = parseSigned(byteBuf);
                    if (currentDataType != null) {
                        rcState = RC_STATE.READ_COUNTER_TYPE;
                    }
                    break;
                case READ_COUNTER_TYPE:
                    if (currentOperateType.getLenValue() > TAG_B) {
                        counterType = parseSigned(byteBuf);
                        if (counterType != null) {
                            rcState = RC_STATE.READ_DATA_ADD;
                        }
                    } else {
                        rcState = RC_STATE.READ_DATA_BASE;
                    }
                    break;
                case READ_DATA_ADD:
                    if ((currentOperateType.getLenValue() & TAG_A) == TAG_A) {
                        Number addNumber = (Number) parseCounter(byteBuf, counterType.getLenValue());
                        if (addNumber == null) {
                            break;
                        }
                        rcValue += addNumber.doubleValue();
                    }
                    rcState = RC_STATE.READ_DATA_DECR;
                    break;
                case READ_DATA_DECR:
                    if ((currentOperateType.getLenValue() & TAG_D) == TAG_D) {
                        Number decrNumber = (Number) parseCounter(byteBuf, counterType.getLenValue());
                        if (decrNumber == null) {
                            break;
                        }
                        rcValue -= decrNumber.doubleValue();
                    }
                    rcState = RC_STATE.READ_DATA_BASE;
                    break;
                case READ_DATA_BASE:
                    if ((currentOperateType.getLenValue() & TAG_B) == TAG_B) {
                        Object baseNumber = parseBase(byteBuf);
                        if (baseNumber == null) {
                            break;
                        }
                        if (baseNumber instanceof Number) {
                            rcValue += ((Number) baseNumber).doubleValue();
                        } else {
                            rcString = baseNumber.toString().getBytes();
                        }
                    }
                    readValueCount++;
                    if (readValueCount == valueLength.getLenValue()) {
                        rcState = RC_STATE.READ_INIT;
                        if (rcString != null) {
                            return rcString;
                        }
                        val = trim(ByteBuffer.allocate(8).putDouble(rcValue).array());
                        return val;
                    } else {
                        rcState = RC_STATE.READ_GID;
                    }
            }
        }

        return val;
    }

    private byte[] trim(byte[] val) {
        double value = ByteBuffer.wrap(val).getDouble();
        if (value % 1 == 0 && value <= Long.MAX_VALUE && value >= Long.MIN_VALUE) {
            return Long.toString((long) value).getBytes();
        }
        return Double.toString(ByteBuffer.wrap(val).getDouble()).getBytes();
    }

    protected byte[] parseVectorClock(ByteBuf byteBuf, int version) {
        while (byteBuf.readableBytes() > 0) {
            switch (vcState) {
                case READ_INIT:
                    vcLength = null;
                    if (version == 0) {
                        vcState = READ_VC_STATE.READ_STR_VC;
                    } else {
                        vcState = READ_VC_STATE.READ_VC_LENGTH;
                    }
                    break;
                case READ_STR_VC:
                    byte[] vcStr = parseString(byteBuf);
                    if (null != vcStr) {
                        vcState = READ_VC_STATE.READ_INIT;
                        return END_MARK.getBytes();
                    }
                    break;
                case READ_VC_LENGTH:
                    vcLength = parseSigned(byteBuf);
                    if (vcLength != null) {
                        if (vcLength.getLenValue() <= 0) {
                            vcState = READ_VC_STATE.READ_INIT;
                            return END_MARK.getBytes();
                        }
                        vcState = READ_VC_STATE.READ_VC_VALUE;
                    }
                    break;
                case READ_VC_VALUE:
                    List<RdbLength> rdbLengths = parseSignedList(byteBuf, vcLength.getLenValue());
                    if (rdbLengths != null) {
                        vcState = READ_VC_STATE.READ_INIT;
                        return END_MARK.getBytes();
                    }
                    break;
            }
        }
        return null;
    }

    protected List<RdbLength> parseSignedList(ByteBuf byteBuf, int count) {
        while (byteBuf.readableBytes() > 0) {
            switch (readListState) {
                case READ_LIST_INIT:
                    valueList = new ArrayList<>();
                    readListState = CRDT_READ_LIST_STATE.READ_LIST_VALUE;
                    break;

                case READ_LIST_VALUE:
                    RdbLength currentValue = parseSigned(byteBuf);
                    if (null != currentValue) {
                        valueList.add(currentValue);
                        if (valueList.size() >= count) {
                            readListState = CRDT_READ_LIST_STATE.READ_LIST_INIT;
                            return valueList;
                        }
                    }
                    break;
            }
        }
        return null;
    }

    protected Double parseDouble(ByteBuf byteBuf) {
        lenNeedBytes = 9;
        while (byteBuf.readableBytes() > 0) {
            lenTemp = readUntilBytesEnough(byteBuf, lenTemp, lenNeedBytes);
            if (lenTemp.readableBytes() == lenNeedBytes) {
                lenTemp.readByte();
                double value = lenTemp.readDoubleLE();
                lenTemp.release();
                lenTemp = null;
                return value;
            }
        }
        return null;
    }

    private READ_VALUE_STATE readValueState = READ_VALUE_STATE.READ_VALUE_INIT;

    private Object parseBase(ByteBuf byteBuf) {
        while (byteBuf.readableBytes() > 0) {
            switch (readValueState) {
                case READ_VALUE_INIT:
                    valueType = null;
                    readValueState = READ_VALUE_STATE.READ_VALUE_TYPE;
                    break;
                case READ_VALUE_TYPE:
                    valueType = loadBaseType(byteBuf);
                    if (valueType != null) {
                        readValueState = READ_VALUE_STATE.READ_VALUE;
                    }
                    break;
                case READ_VALUE:
                    Object value = loadValue(byteBuf, valueType.getLenValue());
                    if (value != null) {
                        readValueState = READ_VALUE_STATE.READ_VALUE_INIT;
                        valueType = null;
                        return value;
                    }
            }
        }
        return null;
    }

    private Object parseCounter(ByteBuf byteBuf, int counterType) {
        while (byteBuf.readableBytes() > 0) {
            switch (readValueState) {
                case READ_VALUE_INIT:
                    readValueState = READ_VALUE_STATE.READ_VALUE_TYPE;
                    break;
                case READ_VALUE_TYPE:
                    RdbLength vc = parseSigned(byteBuf);
                    if (vc != null) {
                        readValueState = READ_VALUE_STATE.READ_VALUE;
                    }
                    break;
                case READ_VALUE:
                    Object value = loadValue(byteBuf, counterType);
                    if (value != null) {
                        readValueState = READ_VALUE_STATE.READ_VALUE_INIT;
                        return value;
                    }
            }
        }
        return null;
    }

    private RdbLength loadBaseType(ByteBuf byteBuf) {
        List<RdbLength> rdbLengths = parseSignedList(byteBuf, 3);
        if (rdbLengths != null) {
            return rdbLengths.get(2);
        }
        return null;
    }

    private Object loadValue(ByteBuf byteBuf, int type) {
        switch (type) {
            case VALUE_TYPE_LONGLONG:
                return loadLong(byteBuf);
            case VALUE_TYPE_DOUBLE:
                return parseDouble(byteBuf);
            case VALUE_TYPE_SDS:
                byte[] byteValue = parseString(byteBuf);
                if (byteValue != null) {
                    try {
                        return Double.parseDouble(new String(byteValue));
                    } catch (Exception e) {
                        return new String(byteValue);
                    }
                }
                return null;
            case VALUE_TYPE_LONGDOUBLE:
                return loadLongDouble(byteBuf);
            case VALUE_TYPE_NONE:
                return 0;
        }
        return null;
    }

    protected Long loadLong(ByteBuf byteBuf) {
        RdbLength longValue = parseSigned(byteBuf);
        if (longValue != null) {
            return longValue.getLenLongValue();
        }
        return null;
    }

    protected Number loadLongDouble(ByteBuf byteBuf) {
        byte[] key = parseString(byteBuf);
        if (null != key) {
            return decode(key);
        }
        return null;
    }

    // 128bit根据IEEE 754规则转换为double
    private Double decode(byte[] bytes) {
        if (bytes.length == 16) {
            bytes = Arrays.copyOfRange(bytes, 0, 10);
            swapBytes(bytes);
            IEEE754 actualIeee = IEEE754.decode(
                    IEEE754Format.QUADRUPLE, BitUtils.wrapSource(bytes));
            if (actualIeee instanceof IEEE754.IEEE754Number) {
                IEEE754.IEEE754Number in = (IEEE754.IEEE754Number) actualIeee;
                Double value = in.getSignificand().doubleValue()
                        * Math.pow(2D, in.getExponent().doubleValue());
                if (!value.isInfinite() && !value.isNaN()) {
                    logger.debug("decode long double value: {} , key", value, context.getKey());
                    return value;
                }
            }
        }
        context.setIncompatibleKey(new String(context.getKey().get()));
        EventMonitor.DEFAULT.logEvent("APPLIER.INCOMPATIBLE.KEY", new String(context.getKey().get()));
        return 0.0;
    }

    void swapBytes(byte[] bytes) {
        int size = bytes.length;
        for (int i = 0; i < size / 2; i++) {
            byte temp = bytes[i];
            bytes[i] = bytes[size - 1 - i];
            bytes[size - 1 - i] = temp;
        }
    }
}
