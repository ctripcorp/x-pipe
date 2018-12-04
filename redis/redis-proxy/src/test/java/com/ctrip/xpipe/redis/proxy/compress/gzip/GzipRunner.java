package com.ctrip.xpipe.redis.proxy.compress.gzip;

import com.ctrip.xpipe.redis.proxy.compress.AbstractRunner;
import com.ctrip.xpipe.redis.proxy.compress.BaseCompressBenmark;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipRunner extends AbstractRunner {

    public GzipRunner(BaseCompressBenmark benmark) {
        super(benmark);
    }

    @Override
    protected OutputStream getCompressOutputStream(OutputStream outputStream) {
        try {
            return new GZIPOutputStream(outputStream);
        } catch (IOException e) {
            logger.error("[getCompressOutputStream]", e);
        }
        throw new IllegalArgumentException("");
    }

    @Override
    protected InputStream getDecompressInputStream(InputStream inputStream) {
        try {
            return new GZIPInputStream(inputStream);
        } catch (IOException e) {
            logger.error("[getDecompressInputStream]", e);
        }
        throw new IllegalArgumentException("");
    }
}
