package com.ctrip.xpipe.redis.console.healthcheck.nonredis.dbvariables.checker;

import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UnreachableMajorityTimeoutChecker extends AbstractVariableChecker {

    @Autowired
    public UnreachableMajorityTimeoutChecker(AlertManager alertManager) {
        super(alertManager);
    }

    boolean checkValue(Object value) {
        return null != value && Integer.parseInt(value.toString()) > 0;
    }

    String getName() {
        return "group_replication_unreachable_majority_timeout";
    }

}
