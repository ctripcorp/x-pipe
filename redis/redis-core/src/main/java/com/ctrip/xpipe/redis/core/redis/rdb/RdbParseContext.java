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

    RdbParser<?> getOrCreateCrdtParser(RdbCrdtType rdbCrdtType);

    void registerListener(RdbParseListener listener);

    void unregisterListener(RdbParseListener listener);

    RdbParseContext setDbId(int dbId);

    int getDbId();

    RdbParseContext setRdbVersion(int version);

    int getRdbVersion();

    RdbParseContext setCurrentType(RdbType rdbType);

    RdbType getCurrentType();

    RdbParseContext setAux(String key, String value);

    String getAux(String key);

    Map<String, String> getAllAux();

    RdbParseContext setKey(RedisKey key);

    RedisKey getKey();

    RdbParseContext setExpireMilli(long expireMilli);

    long getExpireMilli();

    RdbParseContext setLruIdle(long idle);

    long getLruIdle();

    RdbParseContext setLfuFreq(int freq);

    int getLfuFreq();

    void clearKvContext();

    void reset();

    void setCrdt(boolean crdt);

    boolean isCrdt();

    RdbParseContext setCrdtType(RdbCrdtType crdtType);

    RdbCrdtType getCrdtType();

    void clearCrdtType();


    enum RdbType {

        STRING(RdbConstant.REDIS_RDB_TYPE_STRING, false, RdbStringParser::new),
        LIST(RdbConstant.REDIS_RDB_TYPE_LIST, false, RdbListParser::new),
        SET(RdbConstant.REDIS_RDB_TYPE_SET, false, RdbSetParser::new),
        //        ZSET(RdbConstant.REDIS_RDB_TYPE_ZSET),
        HASH(RdbConstant.REDIS_RDB_TYPE_HASH, false, RdbHashParser::new),
        ZSET2(RdbConstant.REDIS_RDB_TYPE_ZSET2, false, RdbZSet2Parser::new),
        //        MODULE(RdbConstant.REDIS_RDB_TYPE_MODULE),
//        MODULE2(RdbConstant.REDIS_RDB_TYPE_MODULE2),
//        HASH_ZIPMAP(RdbConstant.REDIS_RDB_TYPE_HASH_ZIPMAP),
//        LIST_ZIPLIST(RdbConstant.REDIS_RDB_TYPE_LIST_ZIPLIST),
        SET_INTSET(RdbConstant.REDIS_RDB_TYPE_SET_INTSET, false, RdbSetIntSetParser::new),
        ZSET_ZIPLIST(RdbConstant.REDIS_RDB_TYPE_ZSET_ZIPLIST, false, RdbZSetZiplistParser::new),
        HASH_ZIPLIST(RdbConstant.REDIS_RDB_TYPE_HASH_ZIPLIST, false, RdbHashZipListParser::new),
        LIST_QUICKLIST(RdbConstant.REDIS_RDB_TYPE_LIST_QUICKLIST, false, RdbQuickListParser::new),
        STREAM_LISTPACKS(RdbConstant.REDIS_RDB_TYPE_STREAM_LISTPACKS, false, RdbStreamListpacksParser::new),
        CRDT(RdbConstant.REDIS_RDB_TYPE_CRDT, false, DefaultRdbCrdtParser::new),
        //        MODULE_AUX(RdbConstant.REDIS_RDB_OP_CODE_MODULE_AUX),
        IDLE(RdbConstant.REDIS_RDB_OP_CODE_IDLE, true, RdbIdleParser::new),
        FREQ(RdbConstant.REDIS_RDB_OP_CODE_FREQ, true, RdbFreqParser::new),
        AUX(RdbConstant.REDIS_RDB_OP_CODE_AUX, true, RdbAuxParser::new),
        RESIZEDB(RdbConstant.REDIS_RDB_OP_CODE_RESIZEDB, true, RdbResizeDbParser::new),
        EXPIRETIME_MS(RdbConstant.REDIS_RDB_OP_CODE_EXPIRETIME_MS, true, RdbExpiretimeMsParser::new),
        EXPIRETIME(RdbConstant.REDIS_RDB_OP_CODE_EXPIRETIME, true, RdbExpiretimeParser::new),
        SELECTDB(RdbConstant.REDIS_RDB_OP_CODE_SELECTDB, true, RdbSelectDbParser::new),
        EOF(RdbConstant.REDIS_RDB_OP_CODE_EOF, true, null),

        // extend for rordb
        RORDB_SWAP_VERSION(RdbConstant.REDIS_RORDB_OP_CODE_SWAP_VERSION, true, null),
        RORDB_SST(RdbConstant.REDIS_RORDB_OP_CODE_SST, true, null),
        RORDB_KEY_NUM(RdbConstant.REDIS_RORDB_OP_CODE_KEY_NUM, true, null),
        RORDB_CUCKOO_FILTER(RdbConstant.REDIS_RORDB_OP_CODE_CUCKOO_FILTER, true, null),
        RORDB_HASH(RdbConstant.REDIS_RORDB_OP_CODE_HASH, true, null),
        RORDB_SET(RdbConstant.REDIS_RORDB_OP_CODE_SET, true, null),
        RORDB_ZSET(RdbConstant.REDIS_RORDB_OP_CODE_ZSET, true, null),
        RORDB_LIST(RdbConstant.REDIS_RORDB_OP_CODE_LIST, true, null),
        RORDB_BITMAP(RdbConstant.REDIS_RORDB_OP_CODE_BITMAP, true, null),
        RORDB_LIMIT(RdbConstant.REDIS_RORDB_OP_CODE_LIMIT, true, null);

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
            if (null == parserConstructor) {
                throw new UnsupportedOperationException("no parser for " + this.name());
            }
            return parserConstructor.apply(parserManager);
        }

        static {
            for (RdbType rdbType : values()) {
                types.put(rdbType.code, rdbType);
            }
        }

        public static RdbType findByCode(short code) {
            return types.get(code);
        }

    }

    String cSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

    enum RdbCrdtType {
        // "crdt_regr", "crdt_setr", "crdt_hash", "crdt_rc_v", "crdt_ss_v"
        REGISTER(generateTypeId("crdt_regr", 0), false, RdbCrdtRegisterParser::new),
        REGISTER_TOMBSTONE(generateTypeId("crdt_regt", 0), true, RdbCrdtRegisterParser::new),
        ;
        private long typeId;

        private boolean tombstone;
        private Function<RdbParseContext, RdbParser> parserConstructor;

        private static final Map<Long, RdbCrdtType> types = new HashMap<>();

        RdbCrdtType(long code, boolean tombstone, Function<RdbParseContext, RdbParser> parserConstructor) {
            this.typeId = code;
            this.tombstone = tombstone;
            this.parserConstructor = parserConstructor;
        }

        public long getTypeId() {
            return typeId;
        }

        public boolean isTombstone() {
            return tombstone;
        }


        public RdbParser<?> makeParser(RdbParseContext parserManager) {
            if (null == parserConstructor) {
                throw new UnsupportedOperationException("no parser for " + this.name());
            }
            return parserConstructor.apply(parserManager);
        }

        static {
            for (RdbCrdtType rdbType : values()) {
                types.put(rdbType.typeId, rdbType);
            }
        }

        public static RdbCrdtType findByTypeId(long typeId) {
            return types.get(typeId);
        }

        private static long generateTypeId(String name, int encver) {
            long id = 0;
            char[] chars = name.toCharArray();
            for (int i = 0; i < chars.length; i++) {
                int p = cSet.indexOf(chars[i]);
                id = (id << 6) | p;
            }
            id = (id << 10) | encver;
            return id;
        }
    }

}
