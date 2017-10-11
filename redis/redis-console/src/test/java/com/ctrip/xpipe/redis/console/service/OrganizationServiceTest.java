package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;


/**
 * @author chen.zhu
 *
 * Sep 04, 2017
 */

public class OrganizationServiceTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private OrganizationService organizationService;

    @Test
    public void testGetAllOrganizations() {
        organizationService.getAllOrganizations().forEach(org->logger.info("org: {}", org));
    }
}
