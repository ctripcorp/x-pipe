package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.service.DcService;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Jul 27, 2018
 */
public class ProxyModelTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private DcService dcService;

    @Test
    public void testToProxyTblNullPointException() {
        ProxyModel model = new ProxyModel().setActive(false).setUri("PROXYTCP://10.2.131.202:80").setDcName(dcNames[0]);
        ProxyTbl proto = model.toProxyTbl(new DcIdNameMapper.OneTimeMapper(dcService));
        logger.info("[proto] {}", proto);
    }
}