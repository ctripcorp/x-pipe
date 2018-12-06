package com.ctrip.xpipe.redis.proxy.compress;

import com.ctrip.xpipe.redis.proxy.compress.fastlz.FastLzBenchMark;
import com.ctrip.xpipe.redis.proxy.compress.gzip.GzipBenchMark;
import com.ctrip.xpipe.redis.proxy.compress.snappy.SnappyBenmark;
import com.ctrip.xpipe.redis.proxy.compress.zstandard.ZStandardBenchMark;

public class Main {
    public static void main(String[] args) {
        SnappyBenmark snappyBenmark = new SnappyBenmark();
        snappyBenmark.run();
        ZStandardBenchMark zStandardBenchMark = new ZStandardBenchMark();
        zStandardBenchMark.run();
        FastLzBenchMark fastLzBenchMark = new FastLzBenchMark();
        fastLzBenchMark.run();
        GzipBenchMark gzipBenchMark = new GzipBenchMark();
        gzipBenchMark.run();
    }
}
