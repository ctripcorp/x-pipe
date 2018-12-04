package com.ctrip.xpipe.redis.proxy.compress;

import com.ctrip.xpipe.redis.proxy.compress.listeners.CompressRatioRunListener;
import com.ctrip.xpipe.redis.proxy.compress.listeners.TimeRunListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseCompressBenmark implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(BaseCompressBenmark.class);

    public static final String FILE_PATH = "/Users/zhuchen/Develop/compress/dump.rdb";

    public static final String COMPRESS_PREFIX = "/Users/zhuchen/Develop/compress/compress_";

    public static final String DECOMPRESS_PREFIX = "/Users/zhuchen/Develop/compress/decompress_";

    private String raw = FILE_PATH;

    private String compressed = COMPRESS_PREFIX + System.currentTimeMillis();

    private String decompressed = DECOMPRESS_PREFIX + System.currentTimeMillis();

    private Runner runner;

    private RunNotifier notifier;

    public BaseCompressBenmark() {
        try {
            init();
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    public String getRaw() {
        return raw;
    }

    public String getCompressed() {
        return compressed;
    }

    public String getDecompressed() {
        return decompressed;
    }

    @Override
    public void run() {
        try {
            init();
        } catch (Exception e) {
            logger.error("[init]", e);
        }

        try {
            runner.run(notifier);
        } catch (Exception e) {
            logger.error("", e);
        }
        Result result = new Result();
        notifier.registerResult(result);
        result.printResult();
    }

    private void init() {

        runner = getRunner(this);
        notifier = new DefaultRunNotifier();
        notifier.addListener(new TimeRunListener());
        notifier.addListener(new CompressRatioRunListener(this));
        init(runner);
    }


    protected abstract Runner getRunner(BaseCompressBenmark benmark);

    protected abstract void init(Runner runner);

}
