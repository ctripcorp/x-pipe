package com.ctrip.xpipe.redis.proxy.compress;

public interface RunNotifier {

    void addListener(RunListener listener);

    void removeListener(RunListener listener);

    void fireCompressStarted();

    void fireCompressFinished();

    void fireDecompressStarted();

    void fireDecompressFinished();

    void registerResult(Result result);
}
