package com.ctrip.xpipe.redis.proxy.compress.gzip;

import com.ctrip.xpipe.redis.proxy.compress.BaseCompressBenmark;
import com.ctrip.xpipe.redis.proxy.compress.Runner;

public class GzipBenchMark extends BaseCompressBenmark {
    @Override
    protected Runner getRunner(BaseCompressBenmark benmark) {
        return new GzipRunner(benmark);
    }

    @Override
    protected void init(Runner runner) {

    }
}
