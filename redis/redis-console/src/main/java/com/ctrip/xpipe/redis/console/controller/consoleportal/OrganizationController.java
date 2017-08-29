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
 * Created by zhuchen on 2017/8/29.
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

}
