package com.ctrip.xpipe.redis.proxy.compress.fastlz;

import com.ctrip.xpipe.redis.proxy.compress.BaseCompressBenmark;
import com.ctrip.xpipe.redis.proxy.compress.Runner;

public class FastLzBenchMark extends BaseCompressBenmark {
    @Override
    protected Runner getRunner(BaseCompressBenmark benmark) {
        return new FastLzRunner(benmark);
    }

    @Override
    protected void init(Runner runner) {

    }
}
