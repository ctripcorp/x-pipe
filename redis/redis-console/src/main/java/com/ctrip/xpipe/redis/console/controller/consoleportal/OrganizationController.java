package com.ctrip.xpipe.redis.console.controller.consoleportal;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.OrganizationService;

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
}
