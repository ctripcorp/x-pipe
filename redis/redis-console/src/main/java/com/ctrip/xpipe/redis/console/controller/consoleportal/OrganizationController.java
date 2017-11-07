package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author chen.zhu
 *
 * Sep 04, 2017
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class OrganizationController extends AbstractConsoleController {

    @Autowired
    private OrganizationService organizationService;

    @RequestMapping(value = "/organizations", method = RequestMethod.GET)
    public List<OrganizationTbl> getAllOrganizations() {
        return organizationService.getAllOrganizations();
    }

    @RequestMapping(value = "/organization/{id}", method = RequestMethod.GET)
    public List<OrganizationTbl> getOrganizationById(long id) {

        return organizationService.getAllOrganizations();
    }

    @RequestMapping(value = "/involved/organizations", method = RequestMethod.GET)
    public List<OrganizationTbl> getInvolvedOrgs() {
        return organizationService.getInvolvedOrgs();
    }
}
