package com.ctrip.xpipe.redis.checker.controller;


import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.message.forward.ForwardAlertService;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author qifanwang
 * date 2024/6/18
 */

@RestController
public class CheckerMailForwardController {

    @Autowired
    private ForwardAlertService forwardAlertService;

    protected Logger logger = LoggerFactory.getLogger(getClass());


    @PostMapping(ConsoleCheckerPath.PATH_PUT_CHECKER_LEADER_MERGE_ALERT)
    public RetMessage mergeAlerts(@PathVariable String alertType,
                        @RequestBody List<AlertEntity> alertEntities) {
        if(StringUtil.isEmpty("alertType")) {
            throw new IllegalArgumentException("alertType is required");
        }

        boolean status;
        if(alertType.equalsIgnoreCase("alert")) {
            status = forwardAlertService.addAll(true, alertEntities);
        } else {
            status = forwardAlertService.addAll(false, alertEntities);
        }

        if(status) {
            return RetMessage.createSuccessMessage("All alerts have been added successfully.");
        } else {
            return RetMessage.createFailMessage("Failed to add alerts");
        }

    }
}
