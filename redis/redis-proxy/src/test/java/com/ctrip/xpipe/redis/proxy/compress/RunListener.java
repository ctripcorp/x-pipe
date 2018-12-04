package com.ctrip.xpipe.redis.proxy.compress;

public interface RunListener {

    void testCompressStarted();

    void testCompressFinished();

    void testDecompressStarted();

    void testDecompressFinished();

    void statsToResult(Result result);
}
