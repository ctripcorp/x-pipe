package com.ctrip.xpipe.redis.proxy.compress.zstandard;

import com.ctrip.xpipe.redis.proxy.compress.AbstractRunner;
import com.ctrip.xpipe.redis.proxy.compress.BaseCompressBenmark;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ZStandardRunner extends AbstractRunner {

    public ZStandardRunner(BaseCompressBenmark benmark) {
        super(benmark);
    }

    @Override
    protected OutputStream getCompressOutputStream(OutputStream outputStream) {
        try {
            return new ZstdOutputStream(outputStream, 1);
        } catch (IOException e) {
            logger.error("[getCompressOutputStream]", e);
        }
        throw new IllegalArgumentException("");
    }

    @Override
    protected InputStream getDecompressInputStream(InputStream inputStream) {
        try {
            return new ZstdInputStream(inputStream);
        } catch (IOException e) {
            logger.error("[getCompressOutputStream]", e);
        }
        throw new IllegalArgumentException("");
    }

}
