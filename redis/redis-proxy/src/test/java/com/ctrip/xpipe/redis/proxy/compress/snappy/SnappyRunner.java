package com.ctrip.xpipe.redis.proxy.compress.snappy;

import com.ctrip.xpipe.redis.proxy.compress.AbstractRunner;
import com.ctrip.xpipe.redis.proxy.compress.BaseCompressBenmark;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SnappyRunner extends AbstractRunner {

    public SnappyRunner(BaseCompressBenmark benmark) {
        super(benmark);
    }

    @Override
    protected OutputStream getCompressOutputStream(OutputStream outputStream) {
        return new SnappyOutputStream(outputStream);
    }

    @Override
    protected InputStream getDecompressInputStream(InputStream inputStream) {
        try {
            return new SnappyInputStream(inputStream);
        } catch (IOException e) {
            logger.error("[getDecompressInputStream]", e);
        }
        throw new IllegalArgumentException("");
    }

}
