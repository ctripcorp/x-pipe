package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.exception.RdbParseWrongDataException;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parser for RDB_TYPE_MODULE_2.
 * <p>
 * Format:
 * <ul>
 *   <li>moduleid (64-bit length encoded)</li>
 *   <li>module-specific data, terminated by RDB_MODULE_OPCODE_EOF (0)</li>
 * </ul>
 */
public class RdbModuleParser extends AbstractRdbParser<Integer> implements RdbParser<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(RdbModuleParser.class);

    private final RdbParseContext context;
    private final RdbParser<byte[]> rdbStringParser;

    private State state = State.READ_INIT;
    private ModuleDataState moduleDataState = ModuleDataState.READ_OPCODE;
    private long moduleId;
    private long currentOpcode;
    private int pendingFloatBytes;
    private int pendingDoubleBytes;

    private enum State {
        READ_INIT,
        READ_MODULE_ID,
        READ_MODULE_DATA,
        READ_END
    }

    private enum ModuleDataState {
        READ_OPCODE,
        READ_INT,
        READ_FLOAT,
        READ_DOUBLE,
        READ_STRING,
        READ_EOF
    }

    @SuppressWarnings("unchecked")
    public RdbModuleParser(RdbParseContext parseContext) {
        this.context = parseContext;
        this.rdbStringParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
    }

    @Override
    public Integer read(ByteBuf byteBuf) {
        while (!isFinish() && byteBuf.readableBytes() > 0) {
            switch (state) {
                case READ_INIT:
                    moduleId = 0;
                    currentOpcode = 0;
                    pendingFloatBytes = 0;
                    pendingDoubleBytes = 0;
                    moduleDataState = ModuleDataState.READ_OPCODE;
                    state = State.READ_MODULE_ID;
                    break;

                case READ_MODULE_ID:
                    RdbLength moduleIdLen = parseRdbLength(byteBuf);
                    if (moduleIdLen == null) {
                        return null;
                    }
                    moduleId = moduleIdLen.getLenLongValue();
                    state = State.READ_MODULE_DATA;
                    break;

                case READ_MODULE_DATA:
                    if (!parseModuleData(byteBuf)) {
                        return null;
                    }
                    state = State.READ_END;
                    break;

                case READ_END:
                default:
                    break;
            }
        }

        return isFinish() ? 1 : null;
    }

    /**
     * Consume module-specific data until EOF.
     */
    private boolean parseModuleData(ByteBuf byteBuf) {
        while (true) {
            switch (moduleDataState) {
                case READ_OPCODE:
                    RdbLength opcodeLen = parseRdbLength(byteBuf);
                    if (opcodeLen == null) {
                        return false;
                    }
                    currentOpcode = opcodeLen.getLenLongValue();
                    if (currentOpcode == RdbConstant.RDB_MODULE_OPCODE_EOF) {
                        moduleDataState = ModuleDataState.READ_EOF;
                        continue;
                    }
                    switch ((int) currentOpcode) {
                        case RdbConstant.RDB_MODULE_OPCODE_SINT:
                        case RdbConstant.RDB_MODULE_OPCODE_UINT:
                            moduleDataState = ModuleDataState.READ_INT;
                            break;
                        case RdbConstant.RDB_MODULE_OPCODE_FLOAT:
                            pendingFloatBytes = 4;
                            moduleDataState = ModuleDataState.READ_FLOAT;
                            break;
                        case RdbConstant.RDB_MODULE_OPCODE_DOUBLE:
                            pendingDoubleBytes = 8;
                            moduleDataState = ModuleDataState.READ_DOUBLE;
                            break;
                        case RdbConstant.RDB_MODULE_OPCODE_STRING:
                            moduleDataState = ModuleDataState.READ_STRING;
                            break;
                        default:
                            throw new RdbParseWrongDataException("Unknown module opcode: " + currentOpcode);
                    }
                    break;

                case READ_INT:
                    if (parseRdbLength(byteBuf) == null) {
                        return false;
                    }
                    moduleDataState = ModuleDataState.READ_OPCODE;
                    break;

                case READ_FLOAT: {
                    int skipped = skip(byteBuf, pendingFloatBytes);
                    pendingFloatBytes -= skipped;
                    if (pendingFloatBytes > 0) {
                        return false;
                    }
                    moduleDataState = ModuleDataState.READ_OPCODE;
                    break;
                }

                case READ_DOUBLE: {
                    int skipped = skip(byteBuf, pendingDoubleBytes);
                    pendingDoubleBytes -= skipped;
                    if (pendingDoubleBytes > 0) {
                        return false;
                    }
                    moduleDataState = ModuleDataState.READ_OPCODE;
                    break;
                }

                case READ_STRING:
                    byte[] pendingString = rdbStringParser.read(byteBuf);
                    if (pendingString == null) {
                        return false;
                    }
                    rdbStringParser.reset();
                    moduleDataState = ModuleDataState.READ_OPCODE;
                    break;

                case READ_EOF:
                    return true;
            }
        }
    }

    @Override
    public boolean isFinish() {
        return State.READ_END.equals(state);
    }

    @Override
    public void reset() {
        super.reset();
        this.state = State.READ_INIT;
        this.moduleDataState = ModuleDataState.READ_OPCODE;
        this.moduleId = 0;
        this.currentOpcode = 0;
        this.pendingFloatBytes = 0;
        this.pendingDoubleBytes = 0;
        if (rdbStringParser != null) {
            rdbStringParser.reset();
        }
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
