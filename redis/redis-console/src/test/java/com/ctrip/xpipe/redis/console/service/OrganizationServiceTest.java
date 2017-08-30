package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;


/**
 * Created by zhuchen on 2017/8/30.
 */

public class OrganizationServiceTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private OrganizationService organizationService;

    @Test
    public void testGetAllOrganizations() {
        organizationService.getAllOrganizations().forEach(org->logger.info("org: {}", org));
    }
}
