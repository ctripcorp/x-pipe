package com.ctrip.xpipe.redis.core.redis.rdb;

import com.ctrip.xpipe.redis.core.redis.rdb.parser.RdbAuxParser;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.RdbStringParser;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * @author lishanglin
 * date 2022/5/29
 */
public interface RdbParseContext {

    RdbParser getOrCreateParser(RdbType rdbType);

    void registerListener(RdbParseListener listener);

    void unregisterListener(RdbParseListener listener);

    enum RdbType {

        STRING(RdbConstant.REDIS_RDB_TYPE_STRING, RdbStringParser::new),
//        LIST(RdbConstant.REDIS_RDB_TYPE_LIST),
//        SET(RdbConstant.REDIS_RDB_TYPE_SET),
//        ZSET(RdbConstant.REDIS_RDB_TYPE_ZSET),
//        HASH(RdbConstant.REDIS_RDB_TYPE_HASH),
//        ZSET2(RdbConstant.REDIS_RDB_TYPE_ZSET2),
//        MODULE(RdbConstant.REDIS_RDB_TYPE_MODULE),
//        MODULE2(RdbConstant.REDIS_RDB_TYPE_MODULE2),
//        HASH_ZIPMAP(RdbConstant.REDIS_RDB_TYPE_HASH_ZIPMAP),
//        LIST_ZIPLIST(RdbConstant.REDIS_RDB_TYPE_LIST_ZIPLIST),
//        SET_INTSET(RdbConstant.REDIS_RDB_TYPE_SET_INTSET),
//        ZSET_ZIPLIST(RdbConstant.REDIS_RDB_TYPE_ZSET_ZIPLIST),
//        HASH_ZIPLIST(RdbConstant.REDIS_RDB_TYPE_HASH_ZIPLIST),
//        LIST_QUICKLIST(RdbConstant.REDIS_RDB_TYPE_LIST_QUICKLIST),
//        STREAM_LISTPACKS(RdbConstant.REDIS_RDB_TYPE_STREAM_LISTPACKS),
//        MODULE_AUX(RdbConstant.REDIS_RDB_OP_CODE_MODULE_AUX),
//        IDLE(RdbConstant.REDIS_RDB_OP_CODE_IDLE),
//        FREQ(RdbConstant.REDIS_RDB_OP_CODE_FREQ),
        AUX(RdbConstant.REDIS_RDB_OP_CODE_AUX, RdbAuxParser::new),
//        RESIZEDB(RdbConstant.REDIS_RDB_OP_CODE_RESIZEDB),
//        EXPIRETIME_MS(RdbConstant.REDIS_RDB_OP_CODE_EXPIRETIME_MS),
//        EXPIRETIME(RdbConstant.REDIS_RDB_OP_CODE_EXPIRETIME),
//        SELECTDB(RdbConstant.REDIS_RDB_OP_CODE_SELECTDB),
        EOF(RdbConstant.REDIS_RDB_OP_CODE_EOF, null);

        private short code;

        private Function<RdbParseContext, RdbParser> parserConstructor;

        private static final Map<Short, RdbType> types = new HashMap<>();

        RdbType(short code, Function<RdbParseContext, RdbParser> parserConstructor) {
            this.code = code;
            this.parserConstructor = parserConstructor;
        }

        public short getCode() {
            return code;
        }

        public RdbParser<?> makeParser(RdbParseContext parserManager) {
            return parserConstructor.apply(parserManager);
        }

        static {
            for (RdbType rdbType: values()) {
                types.put(rdbType.code, rdbType);
            }
        }

        public static RdbType findByCode(short code) {
            return types.get(code);
        }

    }

}
