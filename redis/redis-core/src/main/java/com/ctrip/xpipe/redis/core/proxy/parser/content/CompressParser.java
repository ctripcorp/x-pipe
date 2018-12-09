package com.ctrip.xpipe.redis.core.proxy.parser.content;

import com.ctrip.xpipe.api.proxy.CompressAlgorithm;

import java.util.Arrays;

public class CompressParser implements ProxyContentParser.SubOptionParser {

    private CompressAlgorithm algorithm;

    @Override
    public String output() {
        return String.format("%s %s %s", ProxyContentParser.ContentType.COMPRESS.name(), algorithm.getType().name(), algorithm.version());
    }

    @Override
    public ProxyContentParser.SubOptionParser parse(String... subOption) {
        if(subOption.length < 2) {
            throw new IllegalArgumentException(String.format("Compress Option not correct %s", Arrays.deepToString(subOption)));
        }
        algorithm = new CompressAlgorithm() {
            @Override
            public String version() {
                return subOption[1];
            }

            @Override
            public AlgorithmType getType() {
                return AlgorithmType.valueOf(subOption[0]);
            }
        };
        return this;
    }

    @Override
    public boolean isImportant() {
        return true;
    }

    public CompressAlgorithm getAlgorithm() {
        return algorithm;
    }

    public CompressParser setAlgorithm(CompressAlgorithm algorithm) {
        this.algorithm = algorithm;
        return this;
    }
}
