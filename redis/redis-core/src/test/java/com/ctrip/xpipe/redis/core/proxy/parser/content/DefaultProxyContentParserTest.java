package com.ctrip.xpipe.redis.core.proxy.parser.content;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.proxy.CompressAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.PROXY_OPTION;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultProxyContentParserTest extends AbstractTest {

    private DefaultProxyContentParser parser = new DefaultProxyContentParser();

    @Test
    public void testOutput() {
        CompressParser compressParser = new CompressParser();
        compressParser.setAlgorithm(new CompressAlgorithm() {
            @Override
            public String version() {
                return "1.0";
            }

            @Override
            public AlgorithmType getType() {
                return AlgorithmType.ZSTD;
            }
        });
        parser.setSubOptionParser(compressParser);
        Assert.assertEquals("CONTENT COMPRESS ZSTD 1.0", parser.output());
    }

    @Test
    public void testRead() {
        long time1 = System.nanoTime();
        parser = (DefaultProxyContentParser) parser.read("CONTENT COMPRESS ZSTD 1.0");
        long time2 = System.nanoTime();
        ProxyContentParser.SubOptionParser subOptionParser = parser.getSubOption();
        long time3 = System.nanoTime();
        ProxyContentParser.ContentType type = parser.getContentType();
        long time4 = System.nanoTime();
        Assert.assertEquals(ProxyContentParser.ContentType.COMPRESS, type);
        long time5 = System.nanoTime();
        Assert.assertTrue(subOptionParser instanceof CompressParser);
        long time6 = System.nanoTime();

        logger.info("{}", time2 - time1);
        logger.info("{}", time3 - time2);
        logger.info("{}", time4 - time3);
        logger.info("{}", time5 - time4);
        logger.info("{}", time6 - time5);
        CompressParser compressParser = (CompressParser) subOptionParser;
        Assert.assertEquals(CompressAlgorithm.AlgorithmType.ZSTD, compressParser.getAlgorithm().getType());
        Assert.assertEquals("1.0", compressParser.getAlgorithm().version());
    }

    @Test
    public void testOption() {
        Assert.assertEquals(PROXY_OPTION.CONTENT, parser.option());
    }

    @Test
    public void testOptionImportant() {
        parser = (DefaultProxyContentParser) parser.read("CONTENT COMPRESS ZSTD 1.0");
        Assert.assertTrue(parser.isImportant());
    }

}