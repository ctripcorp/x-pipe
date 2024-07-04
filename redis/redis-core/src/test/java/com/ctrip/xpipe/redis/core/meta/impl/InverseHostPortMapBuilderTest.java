package com.ctrip.xpipe.redis.core.meta.impl;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Dec 28, 2017
 */
public class InverseHostPortMapBuilderTest extends AbstractRedisTest {

    @Test
    public void testBuilder(){

        logger.info("{}", getXpipeMeta());


    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "file-dao-test.xml";
    }
}
