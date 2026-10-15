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
 * Parser for RDB_OPCODE_MODULE_AUX (247).
 * <p>
 * Format (see redis rdb.c rdbSaveSingleModuleAux / load MODULE_AUX):
 * <ul>
 *   <li>moduleid (len)</li>
 *   <li>when_opcode, must be RDB_MODULE_OPCODE_UINT</li>
 *   <li>when (len) — BEFORE_RDB / AFTER_RDB</li>
 *   <li>module payload: annotated SINT/UINT/FLOAT/DOUBLE/STRING until EOF</li>
 * </ul>
 * Consumes bytes only; does not propagate RedisOps (skip for Applier full sync).
 */
public class RdbModuleAuxParser extends AbstractRdbParser<Integer> implements RdbParser<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(RdbModuleAuxParser.class);

    private final RdbParser<byte[]> rdbStringParser;

    private State state = State.READ_INIT;
    private ModuleDataState moduleDataState = ModuleDataState.READ_OPCODE;

    private long moduleId;
    private long when;
    private long currentOpcode;
    private int pendingFloatBytes;
    private int pendingDoubleBytes;

    private enum State {
        READ_INIT,
        READ_MODULE_ID,
        READ_WHEN_OPCODE,
        READ_WHEN,
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
    public RdbModuleAuxParser(RdbParseContext context) {
        this.rdbStringParser = (RdbParser<byte[]>) context.getOrCreateParser(RdbParseContext.RdbType.STRING);
    }

    @Override
    public Integer read(ByteBuf byteBuf) {
        while (!isFinish() && byteBuf.readableBytes() > 0) {
            switch (state) {
                case READ_INIT:
                    moduleId = 0;
                    when = 0;
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
                    // moduleId is a full 64-bit RedisModuleType id (e.g. RediSearch)
                    moduleId = moduleIdLen.getLenLongValue();
                    state = State.READ_WHEN_OPCODE;
                    break;

                case READ_WHEN_OPCODE:
                    RdbLength whenOpcode = parseRdbLength(byteBuf);
                    if (whenOpcode == null) {
                        return null;
                    }
                    if (whenOpcode.getLenLongValue() != RdbConstant.RDB_MODULE_OPCODE_UINT) {
                        throw new RdbParseWrongDataException(
                                "bad MODULE_AUX when_opcode: " + whenOpcode.getLenLongValue());
                    }
                    state = State.READ_WHEN;
                    break;

                case READ_WHEN:
                    RdbLength whenLen = parseRdbLength(byteBuf);
                    if (whenLen == null) {
                        return null;
                    }
                    when = whenLen.getLenLongValue();
                    getLogger().debug("[read][MODULE_AUX] moduleId={}, when={}", moduleId, when);
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
     * Consume annotated module payload until EOF (same framing as rdbLoadCheckModuleValue).
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
                            throw new RdbParseWrongDataException(
                                    "Unknown MODULE_AUX opcode: " + currentOpcode);
                    }
                    break;

                case READ_INT:
                    // discard signed/unsigned int (may be 64-bit encoded)
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
                    byte[] str = rdbStringParser.read(byteBuf);
                    if (str == null) {
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
        this.when = 0;
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
