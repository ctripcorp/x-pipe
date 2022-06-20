package com.ctrip.xpipe.redis.core.redis.rdb;

import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.*;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * @author lishanglin
 * date 2022/5/29
 */
public interface RdbParseContext {

    void bindRdbParser(RdbParser<?> parser);

    RdbParser<?> getOrCreateParser(RdbType rdbType);

    void registerListener(RdbParseListener listener);

    void unregisterListener(RdbParseListener listener);

    RdbParseContext setDbId(int dbId);

    int getDbId();

    RdbParseContext setAux(String key, String value);

    String getAux(String key);

    RdbParseContext setKey(RedisKey key);

    RedisKey getKey();

    RdbParseContext setExpireMilli(long expireMilli);

    long getExpireMilli();

    RdbParseContext setLruIdle(long idle);

    long getLruIdle();

    RdbParseContext setLfuFreq(int freq);

    int getLfuFreq();

    void clearKvContext();

    enum RdbType {

        STRING(RdbConstant.REDIS_RDB_TYPE_STRING, false, RdbStringParser::new),
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
        IDLE(RdbConstant.REDIS_RDB_OP_CODE_IDLE, true, RdbIdleParser::new),
        FREQ(RdbConstant.REDIS_RDB_OP_CODE_FREQ, true, RdbFreqParser::new),
        AUX(RdbConstant.REDIS_RDB_OP_CODE_AUX, true, RdbAuxParser::new),
        RESIZEDB(RdbConstant.REDIS_RDB_OP_CODE_RESIZEDB, true, RdbResizeDbParser::new),
//        EXPIRETIME_MS(RdbConstant.REDIS_RDB_OP_CODE_EXPIRETIME_MS),
//        EXPIRETIME(RdbConstant.REDIS_RDB_OP_CODE_EXPIRETIME),
        SELECTDB(RdbConstant.REDIS_RDB_OP_CODE_SELECTDB, true, RdbSelectDbParser::new),
        EOF(RdbConstant.REDIS_RDB_OP_CODE_EOF, true, null);

        private short code;

        private boolean rdbOp;

        private Function<RdbParseContext, RdbParser> parserConstructor;

        private static final Map<Short, RdbType> types = new HashMap<>();

        RdbType(short code, boolean rdbOp, Function<RdbParseContext, RdbParser> parserConstructor) {
            this.code = code;
            this.rdbOp = rdbOp;
            this.parserConstructor = parserConstructor;
        }

        public short getCode() {
            return code;
        }

        public boolean isRdbOp() {
            return rdbOp;
        }

        public RdbParser<?> makeParser(RdbParseContext parserManager) {
            if (null == parserConstructor) throw new UnsupportedOperationException("no parser for " + this.name());
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