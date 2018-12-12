package com.ctrip.xpipe.api.proxy;

/**
 * @author chen.zhu
 * <p>
 * Aug 03, 2018
 */
public interface CompressAlgorithm {

    String version();

    AlgorithmType getType();

    enum AlgorithmType {
        ZSTD;
    }
}
