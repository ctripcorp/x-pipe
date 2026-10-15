package com.ctrip.xpipe.redis.core.redis.rdb;

/**
 * RDB constants aligned with redis/src/rdb.h (Redis 8.10 / RDB_VERSION 15).
 *
 * @author lishanglin
 * date 2022/5/28
 */
public class RdbConstant {

    private RdbConstant() {

    }

    /**
     * Historical default used by older x-pipe code paths.
     * Live RDB files from Redis 8.10 use version 15 ({@code RDB_VERSION} in rdb.h).
     */
    public static final int REDIS_RDB_VERSION = 9;
    public static final byte[] REDIS_RDB_MAGIC = new byte[]{'R', 'E', 'D', 'I', 'S'};

    // Length encoding (rdb.h: RDB_6BITLEN / RDB_14BITLEN / RDB_32BITLEN / RDB_64BITLEN / RDB_ENCVAL)
    // java no unsigned byte, use short instead
    public static final short REDIS_RDB_LEN_6BITLEN = 0b00;   // RDB_6BITLEN
    public static final short REDIS_RDB_LEN_14BITLEN = 0b01;  // RDB_14BITLEN
    public static final short REDIS_RDB_LEN_LONG = 0b10;      // prefix for 32/64-bit len
    public static final short REDIS_RDB_LEN_32BITLEN = 0x80;  // RDB_32BITLEN
    public static final short REDIS_RDB_LEN_64BITLEN = 0x81;  // RDB_64BITLEN
    public static final short REDIS_RDB_LEN_ENCVAL = 0b11;    // RDB_ENCVAL
    public static final short REDIS_RDB_ENC_INT8 = 0b00;      // RDB_ENC_INT8
    public static final short REDIS_RDB_ENC_INT16 = 0b01;     // RDB_ENC_INT16
    public static final short REDIS_RDB_ENC_INT32 = 0b10;     // RDB_ENC_INT32
    public static final short REDIS_RDB_ENC_LZF = 0b11;       // RDB_ENC_LZF

    // Object types (rdb.h RDB_TYPE_*)
    public static final short REDIS_RDB_TYPE_STRING = 0;
    public static final short REDIS_RDB_TYPE_LIST = 1;
    public static final short REDIS_RDB_TYPE_SET = 2;
    public static final short REDIS_RDB_TYPE_ZSET = 3;
    public static final short REDIS_RDB_TYPE_HASH = 4;
    public static final short REDIS_RDB_TYPE_ZSET2 = 5;              // RDB_TYPE_ZSET_2
    public static final short REDIS_RDB_TYPE_MODULE = 6;             // RDB_TYPE_MODULE_PRE_GA
    public static final short REDIS_RDB_TYPE_MODULE_2 = 7;
    public static final short REDIS_RDB_TYPE_HASH_ZIPMAP = 9;
    public static final short REDIS_RDB_TYPE_LIST_ZIPLIST = 10;
    public static final short REDIS_RDB_TYPE_SET_INTSET = 11;
    public static final short REDIS_RDB_TYPE_ZSET_ZIPLIST = 12;
    public static final short REDIS_RDB_TYPE_HASH_ZIPLIST = 13;
    public static final short REDIS_RDB_TYPE_LIST_QUICKLIST = 14;
    public static final short REDIS_RDB_TYPE_STREAM_LISTPACKS = 15;

    // Ctrip / legacy extensions (not in upstream rdb.h)
    public static final short REDIS_RDB_TYPE_BITMAP = 192;
    /** RDB version 9 era bitmap type code; conflicts with HASH_LISTPACK=16 on newer RDB. */
    public static final short REDIS_RDB_TYPE_BITMAP_9 = 16;
    public static final short REDIS_RDB_TYPE_CRDT = 200;

    // Types added since Redis 7.x / 8.x (rdb.h)
    public static final short REDIS_RDB_TYPE_HASH_LISTPACK            = 16;   // 0x10
    public static final short REDIS_RDB_TYPE_ZSET_LISTPACK            = 17;   // 0x11
    public static final short REDIS_RDB_TYPE_LIST_QUICKLIST_2         = 18;   // 0x12
    public static final short REDIS_RDB_TYPE_STREAM_LISTPACKS_2       = 19;   // 0x13
    public static final short REDIS_RDB_TYPE_SET_LISTPACK             = 20;   // 0x14
    public static final short REDIS_RDB_TYPE_STREAM_LISTPACKS_3       = 21;   // 0x15
    public static final short REDIS_RDB_TYPE_HASH_METADATA_PRE_GA     = 22;   // 0x16
    public static final short REDIS_RDB_TYPE_HASH_LISTPACK_EX_PRE_GA  = 23;   // 0x17
    public static final short REDIS_RDB_TYPE_HASH_METADATA            = 24;   // 0x18
    public static final short REDIS_RDB_TYPE_HASH_LISTPACK_EX         = 25;   // 0x19
    public static final short REDIS_RDB_TYPE_STREAM_LISTPACKS_4       = 26;   // 0x1A
    public static final short REDIS_RDB_TYPE_STREAM_LISTPACKS_5       = 27;   // 0x1B
    public static final short REDIS_RDB_TYPE_ARRAY                    = 28;   // 0x1C
    public static final short REDIS_RDB_TYPE_HASH_TMPL_LP             = 29;   // 0x1D
    public static final short REDIS_RDB_TYPE_HASH_TMPL_LP_REF         = 30;   // 0x1E
    public static final short REDIS_RDB_TYPE_HASH_TMPL_ARRAY          = 31;   // 0x1F
    public static final short REDIS_RDB_TYPE_HASH_TMPL_ARRAY_REF      = 32;   // 0x20
    /** Only present when Redis is built with -DENABLE_GCRA. */
    public static final short REDIS_RDB_TYPE_GCRA                     = 33;   // 0x21

    // KEY_META attribute ids (keymeta.h)
    public static final int KEY_META_ID_EXPIRE = 0;
    public static final int KEY_META_ID_LRU    = 1;
    public static final int KEY_META_ID_LFU    = 2;

    // QUICKLIST node container types (quicklist.h)
    public static final int QUICKLIST_NODE_CONTAINER_PLAIN  = 1;
    public static final int QUICKLIST_NODE_CONTAINER_PACKED = 2;

    // MODULE / MODULE_AUX sub-opcodes (rdb.h RDB_MODULE_OPCODE_*)
    public static final int RDB_MODULE_OPCODE_EOF     = 0;
    public static final int RDB_MODULE_OPCODE_SINT    = 1;
    public static final int RDB_MODULE_OPCODE_UINT    = 2;
    public static final int RDB_MODULE_OPCODE_FLOAT   = 3;
    public static final int RDB_MODULE_OPCODE_DOUBLE  = 4;
    public static final int RDB_MODULE_OPCODE_STRING  = 5;

    // extend for rordb (ctrip)
    public static final short REDIS_RORDB_OP_CODE_SWAP_VERSION = 128;
    public static final short REDIS_RORDB_OP_CODE_SST = 129;
    public static final short REDIS_RORDB_OP_CODE_KEY_NUM = 130;
    public static final short REDIS_RORDB_OP_CODE_CUCKOO_FILTER = 131;
    public static final short REDIS_RORDB_OP_CODE_HASH = 132;
    public static final short REDIS_RORDB_OP_CODE_SET = 133;
    public static final short REDIS_RORDB_OP_CODE_ZSET = 134;
    public static final short REDIS_RORDB_OP_CODE_LIST = 135;
    public static final short REDIS_RORDB_OP_CODE_BITMAP = 136;
    public static final short REDIS_RORDB_OP_CODE_LIMIT = 137;

    // Special RDB opcodes (rdb.h RDB_OPCODE_*)
    public static final short RDB_OPCODE_HASH_TEMPLATE        = 242;  // 0xF2
    public static final short RDB_OPCODE_KEY_META             = 243;  // 0xF3
    public static final short RDB_OPCODE_SLOT_INFO            = 244;  // 0xF4
    public static final short RDB_OPCODE_FUNCTION2            = 245;  // 0xF5
    public static final short RDB_OPCODE_FUNCTION_PRE_GA      = 246;  // 0xF6
    public static final short REDIS_RDB_OP_CODE_MODULE_AUX    = 247;  // 0xF7
    public static final short REDIS_RDB_OP_CODE_IDLE          = 248;  // 0xF8
    public static final short REDIS_RDB_OP_CODE_FREQ          = 249;  // 0xF9
    public static final short REDIS_RDB_OP_CODE_AUX           = 250;  // 0xFA
    public static final short REDIS_RDB_OP_CODE_RESIZEDB      = 251;  // 0xFB
    public static final short REDIS_RDB_OP_CODE_EXPIRETIME_MS = 252;  // 0xFC
    public static final short REDIS_RDB_OP_CODE_EXPIRETIME    = 253;  // 0xFD
    public static final short REDIS_RDB_OP_CODE_SELECTDB      = 254;  // 0xFE
    public static final short REDIS_RDB_OP_CODE_EOF           = 255;  // 0xFF

    public static final String REDIS_RDB_AUX_KEY_GTID = "gtid";
    public static final String REDIS_RDB_AUX_KEY_RORDB = "rordb";

    public static final String REDIS_RDB_AUX_KEY_REPL_MODE = "gtid-repl-mode";
    public static final String REDIS_RDB_AUX_KEY_GTID_EXECUTED = "gtid-executed";
    public static final String REDIS_RDB_AUX_KEY_GTID_LOST = "gtid-lost";

}
