package com.ctrip.xpipe.redis.proxy.compress.zstandard;

import com.ctrip.xpipe.redis.proxy.compress.BaseCompressBenmark;
import com.ctrip.xpipe.redis.proxy.compress.Runner;

public class ZStandardBenchMark extends BaseCompressBenmark {

    @Override
    protected Runner getRunner(BaseCompressBenmark benmark) {
        return new ZStandardRunner(benmark);
    }

    @Override
    protected void init(Runner runner) {

    }
}
