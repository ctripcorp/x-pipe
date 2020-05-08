package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.api.sso.UserInfo;
import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.redis.console.cluster.ConsoleCrossDcServer;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.LeaseModel;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.spring.AbstractController;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;

@RestController
@RequestMapping(AbstractController.API_PREFIX)
public class CrossDcController extends AbstractConsoleController {

    @Autowired(required = false)
    private ConsoleCrossDcServer crossDcClusterServer;

    @Autowired
    private DcService dcService;

    private final static int DEFAULT_LEASE_PERIOD = 10;

    @RequestMapping(value = "/cross-dc/lease", method = RequestMethod.POST)
    public RetMessage updateCrossDcLeaderLease(HttpServletRequest request, @RequestBody LeaseModel leaseModel) {
        if (null == crossDcClusterServer) return RetMessage.createSuccessMessage();

        if (StringUtil.isEmpty(leaseModel.getOwner())) {
            return RetMessage.createFailMessage("invalid lease owner");
        }

        String dcName = leaseModel.getOwner().toUpperCase();
        if (null == dcService.find(dcName)) {
            return RetMessage.createFailMessage("dc not found");
        }

        String sourceIp = request.getHeader("X-FORWARDED-FOR");
        if(sourceIp == null) {
            sourceIp = request.getRemoteAddr();
        }
        Integer validPeriod = leaseModel.getValidPeriod();
        if (null == validPeriod || validPeriod < 0) {
            validPeriod = DEFAULT_LEASE_PERIOD;
        }

        UserInfo user = UserInfoHolder.DEFAULT.getUser();
        String userId = user.getUserId();

        ConfigModel configModel = new ConfigModel();
        configModel.setUpdateUser(userId)
                .setUpdateIP(sourceIp)
                .setVal(dcName);

        try {
            crossDcClusterServer.forceSetCrossLeader(configModel, DateTimeUtils.getMinutesLaterThan(new Date(), validPeriod));
        } catch (Exception e) {
            return RetMessage.createSuccessMessage(e.getMessage());
        }

        return RetMessage.createSuccessMessage();
    }

    @RequestMapping(value = "/cross-dc/lease/refresh", method = RequestMethod.POST)
    public RetMessage refreshCrossDcLeaderLease() {
        if (null == crossDcClusterServer) return RetMessage.createSuccessMessage();

        try {
            crossDcClusterServer.refreshCrossLeaderStatus();
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

}
