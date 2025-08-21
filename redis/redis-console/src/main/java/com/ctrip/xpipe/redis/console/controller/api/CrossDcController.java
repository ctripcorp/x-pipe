package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.sso.UserInfo;
import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.cluster.ConsoleCrossDcServer;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.spring.AbstractController;
import com.ctrip.xpipe.utils.DateTimeUtils;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
@RequestMapping(AbstractController.API_PREFIX)
public class CrossDcController extends AbstractConsoleController {

    @Autowired(required = false)
    private ConsoleCrossDcServer crossDcClusterServer;

    private static final int DEFAULT_LEASE_PERIOD = 10;

    @RequestMapping(value = {"/cross-dc/leader/force/{validPeriod}", "/cross-dc/leader/force"}, method = RequestMethod.POST)
    public RetMessage updateCrossDcLeaderLease(HttpServletRequest request, @PathVariable(required = false) Integer validPeriod) {
        if (null == crossDcClusterServer) return RetMessage.createSuccessMessage();

        String dcName = FoundationService.DEFAULT.getDataCenter();
        String sourceIp = request.getHeader("X-FORWARDED-FOR");
        if(sourceIp == null) {
            sourceIp = request.getRemoteAddr();
        }
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

    @RequestMapping(value = "/cross-dc/leader/refresh", method = RequestMethod.POST)
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
