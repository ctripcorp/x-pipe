package com.ctrip.xpipe.service.organization;

import com.ctrip.xpipe.service.AbstractServiceTest;
import org.junit.Before;
import org.junit.Test;

/**
 * @author chen.zhu
 *
 * Sep 04, 2017
 */
public class CtripOrganizationConfigTest extends AbstractServiceTest {
    CtripOrganizationConfig config;

    @Before
    public void beforeCtripOrganizationConfigTest() {
        config = new CtripOrganizationConfig();
    }

    @Test
    public void testGetCmsOrganizationUrl() {
        String url = config.getCmsOrganizationUrl();
        logger.info("CMS System REST url is: {}", url);
    }

    @Test
    public void testGetCmsAccessToken() {
        String token = config.getCmsAccessToken();
        logger.info("CMS System access token is: {}", token);
    }
}
