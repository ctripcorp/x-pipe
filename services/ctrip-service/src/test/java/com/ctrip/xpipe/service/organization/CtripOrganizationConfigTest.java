package com.ctrip.xpipe.service.organization;

import com.ctrip.xpipe.AbstractServiceTest;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by zhuchen on 2017/8/31.
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
