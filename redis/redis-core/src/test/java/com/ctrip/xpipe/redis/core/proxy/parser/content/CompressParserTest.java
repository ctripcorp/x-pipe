package com.ctrip.xpipe.redis.core.proxy.parser.content;

import com.ctrip.xpipe.api.proxy.CompressAlgorithm;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CompressParserTest {

    private CompressParser parser = new CompressParser();

    @Test
    public void testOutput() {
        parser.setAlgorithm(new CompressAlgorithm() {
            @Override
            public String version() {
                return "1.0";
            }

            @Override
            public AlgorithmType getType() {
                return AlgorithmType.ZSTD;
            }
        });
        Assert.assertEquals("COMPRESS ZSTD 1.0", parser.output());
    }

    @Test
    public void testParse() {
        parser = (CompressParser) parser.parse(new String[]{"ZSTD", "1.0"});
        Assert.assertEquals(CompressAlgorithm.AlgorithmType.ZSTD, parser.getAlgorithm().getType());
        Assert.assertEquals("1.0", parser.getAlgorithm().version());
    }

    @Test
    public void testGetAlgorithm() {
    }

    @Test
    public void testSetAlgorithm() {
        String version = "3.0";
        parser.setAlgorithm(new CompressAlgorithm() {
            @Override
            public String version() {
                return version;
            }

            @Override
            public AlgorithmType getType() {
                return AlgorithmType.ZSTD;
            }
        });
        Assert.assertNotNull(parser.getAlgorithm());
        Assert.assertEquals(version, parser.getAlgorithm().version());
    }
}