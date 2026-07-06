package com.ctrip.xpipe.redis.keeper.storage;

class StorageUtil {

    static String fileKey(String path) {
        return path;
    }

    static String segmentKey(String path, String prefix) {
        return path + "\0" + prefix;
    }
}
