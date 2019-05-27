package com.ctrip.xpipe.redis.proxy.compress.listeners;

import com.ctrip.xpipe.redis.proxy.compress.Result;
import com.ctrip.xpipe.redis.proxy.compress.RunListener;

import java.util.concurrent.atomic.AtomicLong;

public class TimeRunListener implements RunListener {

    private AtomicLong startCompress = new AtomicLong();

    private AtomicLong finishCompress = new AtomicLong();

    private AtomicLong startDecompress = new AtomicLong();

    private AtomicLong finishDecompress = new AtomicLong();

    @Override
    public void testCompressStarted() {
        startCompress.set(System.nanoTime());
    }

    @Override
    public void testCompressFinished() {
        finishCompress.set(System.nanoTime());
    }

    @Override
    public void testDecompressStarted() {
        startDecompress.set(System.nanoTime());
    }

    @Override
    public void testDecompressFinished() {
        finishDecompress.set(System.nanoTime());
    }

    @Override
    public void statsToResult(Result result) {
        result.setCompressInterval(new Result.TimeInterval(startCompress.get(), finishCompress.get()));
        result.setDecompressInterval(new Result.TimeInterval(startDecompress.get(), finishDecompress.get()));
    }
}
