package com.ctrip.xpipe.redis.proxy.compress.listeners;

import com.ctrip.xpipe.redis.proxy.compress.BaseCompressBenmark;
import com.ctrip.xpipe.redis.proxy.compress.Result;
import com.ctrip.xpipe.redis.proxy.compress.RunListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

public class CompressRatioRunListener implements RunListener {

    private static final Logger logger = LoggerFactory.getLogger(CompressRatioRunListener.class);

    private BaseCompressBenmark benmark;

    private AtomicLong compressSize = new AtomicLong();

    public CompressRatioRunListener(BaseCompressBenmark benmark) {
        this.benmark = benmark;
    }

    @Override
    public void testCompressStarted() {

    }

    @Override
    public void testCompressFinished() {
        File file = new File(benmark.getCompressed());
        logger.info("file: {}", file.getName());
        compressSize.set(file.length());
    }

    @Override
    public void testDecompressStarted() {

    }

    @Override
    public void testDecompressFinished() {

    }

    @Override
    public void statsToResult(Result result) {
        result.setCompressedSize(compressSize.get());
    }
}
