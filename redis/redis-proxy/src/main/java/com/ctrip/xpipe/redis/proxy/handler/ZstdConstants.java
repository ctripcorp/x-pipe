package com.ctrip.xpipe.redis.proxy.handler;

public class ZstdConstants {

    static final int MAGIC_NUMBER = 'Z' << 24 | 'S' << 16 | 'T' << 8  | 'D';

    static final int HEADER_LENGTH = 4 +  // magic number
            1 +  // type
            4 +  // compressed length
            4 +  // decompressed length
            4;   // checksum

    static final int DEFAULT_COMPRESS_LEVEL = 0;

    static final int TOKEN_OFFSET = 4;
    static final int COMPRESSED_LENGTH_OFFSET = TOKEN_OFFSET + 1;
    static final int DECOMPRESSED_LENGTH_OFFSET = COMPRESSED_LENGTH_OFFSET + 4;
    static final int CHECKSUM_OFFSET = DECOMPRESSED_LENGTH_OFFSET + 4;


    static final int COMPRESSION_LEVEL_BASE = 10;


    static final int MIN_BLOCK_SIZE = 64;
    static final int MAX_BLOCK_SIZE = 1 << COMPRESSION_LEVEL_BASE + 0x0F;   //  32 M
    static final int DEFAULT_BLOCK_SIZE = 1 << 16;  // 64 KB

    static final int BLOCK_TYPE_NON_COMPRESSED = 0x10;
    static final int BLOCK_TYPE_COMPRESSED = 0x20;


    private ZstdConstants() { }
}
