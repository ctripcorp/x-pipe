package com.ctrip.xpipe.redis.console.healthcheck.nonredis.dbvariables.checker;

import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ExitActionChecker extends AbstractVariableChecker {

    @Autowired
    public ExitActionChecker(AlertManager alertManager) {
        super(alertManager);
    }

    boolean checkValue(Object value) {
        return String.valueOf(value).equalsIgnoreCase("ABORT_SERVER");
    }

    String getName() {
        return "group_replication_exit_state_action";
    }

}
