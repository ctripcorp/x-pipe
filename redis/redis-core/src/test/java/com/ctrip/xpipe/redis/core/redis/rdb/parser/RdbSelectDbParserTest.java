package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Before;
import org.junit.Test;

/**
 * @author lishanglin
 * date 2022/6/4
 */
public class RdbSelectDbParserTest extends AbstractTest {

    private RdbSelectDbParser selectDbParser;

    private byte[] rdbBytesSelectDb = new byte[] {};

    @Before
    public void setupRdbSelectDbParserTest() {
        selectDbParser = new RdbSelectDbParser(new DefaultRdbParseContext());
    }

    @Test
    public void testParse() {

    }

}
