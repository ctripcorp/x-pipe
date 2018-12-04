package com.ctrip.xpipe.redis.proxy.compress.snappy;

import com.ctrip.xpipe.redis.proxy.compress.BaseCompressBenmark;
import com.ctrip.xpipe.redis.proxy.compress.Runner;

public class SnappyBenmark extends BaseCompressBenmark {

    @Override
    protected Runner getRunner(BaseCompressBenmark benmark) {
        return new SnappyRunner(benmark);
    }

    @Override
    protected void init(Runner runner) {

    }

}
