package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * Apr 03, 2018
 */
public class DcMetaServiceImplTest extends AbstractConsoleIntegrationTest {

//    @Autowired
    DcMetaServiceImpl service;

    @Test
    public void getDcMeta() throws Exception {
        long start = System.currentTimeMillis();
        service.getDcMeta(dcNames[0]);
        long end = System.currentTimeMillis();
        logger.info("[duration] {}", end - start);
    }

}