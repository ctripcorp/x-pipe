package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class OrganizationApiController {

    @Autowired
    private OrganizationService organizationService;

    @RequestMapping(value = "/organizations/all", method = RequestMethod.GET)
    public List<OrganizationTbl> getOrganizationById() {
        return organizationService.getAllOrganizations();
    }
}
