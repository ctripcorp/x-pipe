package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * Created by zhuchen on 2017/8/29.
 */
public class OrganizationServiceImplTest {

    @Autowired
    private OrganizationServiceImpl organizationService;


    @Test
    public void testRetrieveOrgInfoFromRemote() {
        List<OrganizationTbl> result = organizationService
                                        .retrieveOrgInfoFromRemote();
    }
}
