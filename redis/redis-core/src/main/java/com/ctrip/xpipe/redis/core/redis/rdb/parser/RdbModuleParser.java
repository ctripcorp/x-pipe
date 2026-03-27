package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.exception.RdbParseEmptyKeyException;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseContext;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author TB
 * @date 2026/3/24 19:57
 */


/**
 *
 * Parser for RDB_TYPE_MODULE_2.
 * <p>
 * Format:
 * <ul>
 *   <li>moduleid (64-bit length encoded)</li>
 *   <li>module-specific data, terminated by RDB_MODULE_OPCODE_EOF (0)</li>
 * </ul>
 * </p>
 */
public class RdbModuleParser extends AbstractRdbParser<Integer> implements RdbParser<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(RdbModuleParser.class);

    private RdbParseContext context;
    private RdbParser<byte[]> rdbStringParser;

    // State machine states
    private State state = State.READ_INIT;
    // Sub-state for module data parsing
    private ModuleDataState moduleDataState = ModuleDataState.READ_OPCODE;
    private long moduleId;
    private long currentOpcode;
    // For reading string when data is chunked
    private byte[] pendingString;
    // For reading float/double when data is chunked
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
                    pendingString = null;
                    pendingFloatBytes = 0;
                    pendingDoubleBytes = 0;
                    moduleDataState = ModuleDataState.READ_OPCODE;
                    state = State.READ_MODULE_ID;
                    break;

                case READ_MODULE_ID:
                    RdbLength moduleIdLen = parseRdbLength(byteBuf);
                    if (moduleIdLen == null) {
                        return null; // need more data
                    }
                    moduleId = moduleIdLen.getLenValue();
                    state = State.READ_MODULE_DATA;
                    break;

                case READ_MODULE_DATA:
                    // Process module data until EOF
                    if (!parseModuleData(byteBuf)) {
                        return null; // need more data
                    }
                    // After module data is fully parsed, we are at the end of the module value
                    state = State.READ_END;
                    break;

                case READ_END:
                default:
                    // do nothing
            }
        }

        if (isFinish()) {
            return 1; // indicate one module parsed
        } else {
            return null;
        }
    }

    /**
     * Parses module-specific data from the stream, consuming bytes until the EOF marker.
     *
     * @param byteBuf the buffer to read from
     * @return true if the module data was fully consumed, false if more data is needed
     */
    private boolean parseModuleData(ByteBuf byteBuf) {
        while (true) {
            switch (moduleDataState) {
                case READ_OPCODE:
                    // Read the next opcode (length encoded)
                    RdbLength opcodeLen = parseRdbLength(byteBuf);
                    if (opcodeLen == null) {
                        return false; // need more data
                    }
                    currentOpcode = opcodeLen.getLenValue();
                    if (currentOpcode == 0) {
                        // EOF marker
                        moduleDataState = ModuleDataState.READ_EOF;
                        continue;
                    }
                    // Determine what to read next based on opcode
                    switch ((int) currentOpcode) {
                        case 1: // RDB_MODULE_OPCODE_SINT
                        case 2: // RDB_MODULE_OPCODE_UINT
                            moduleDataState = ModuleDataState.READ_INT;
                            break;
                        case 3: // RDB_MODULE_OPCODE_FLOAT
                            pendingFloatBytes = 4;
                            moduleDataState = ModuleDataState.READ_FLOAT;
                            break;
                        case 4: // RDB_MODULE_OPCODE_DOUBLE
                            pendingDoubleBytes = 8;
                            moduleDataState = ModuleDataState.READ_DOUBLE;
                            break;
                        case 5: // RDB_MODULE_OPCODE_STRING
                            moduleDataState = ModuleDataState.READ_STRING;
                            break;
                        default:
                            throw new RdbParseEmptyKeyException("Unknown module opcode: " + currentOpcode);
                    }
                    break;

                case READ_INT:
                    // Read integer value (signed or unsigned) and discard
                    RdbLength intVal = parseRdbLength(byteBuf);
                    if (intVal == null) {
                        return false; // need more data
                    }
                    moduleDataState = ModuleDataState.READ_OPCODE;
                    break;

                case READ_FLOAT:
                    if (byteBuf.readableBytes() < pendingFloatBytes) {
                        return false; // need more data
                    }
                    byteBuf.skipBytes(pendingFloatBytes);
                    moduleDataState = ModuleDataState.READ_OPCODE;
                    break;

                case READ_DOUBLE:
                    if (byteBuf.readableBytes() < pendingDoubleBytes) {
                        return false;
                    }
                    byteBuf.skipBytes(pendingDoubleBytes);
                    moduleDataState = ModuleDataState.READ_OPCODE;
                    break;

                case READ_STRING:
                    // Use the string parser to read the string content
                    pendingString = rdbStringParser.read(byteBuf);
                    if (pendingString == null) {
                        return false; // need more data
                    }
                    rdbStringParser.reset();
                    moduleDataState = ModuleDataState.READ_OPCODE;
                    break;

                case READ_EOF:
                    // End of module data reached
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
        this.pendingString = null;
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
