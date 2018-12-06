package com.ctrip.xpipe.redis.proxy.compress.fastlz;

import com.ctrip.xpipe.redis.proxy.compress.AbstractRunner;
import com.ctrip.xpipe.redis.proxy.compress.BaseCompressBenmark;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

import java.io.InputStream;
import java.io.OutputStream;

public class FastLzRunner extends AbstractRunner {

    public FastLzRunner(BaseCompressBenmark benmark) {
        super(benmark);
    }

    @Override
    protected OutputStream getCompressOutputStream(OutputStream outputStream) {
        return new LZ4BlockOutputStream(outputStream);
    }

    @Override
    protected InputStream getDecompressInputStream(InputStream inputStream) {
        return new LZ4BlockInputStream(inputStream);
    }
}
