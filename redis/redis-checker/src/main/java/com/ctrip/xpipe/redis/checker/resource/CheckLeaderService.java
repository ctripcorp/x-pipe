package com.ctrip.xpipe.redis.checker.resource;

import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.cluster.AllCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.util.annotation.Nullable;

import java.util.List;

@Component
public class CheckLeaderService extends AbstractService {

    private AllCheckerLeaderElector elector;

    public CheckLeaderService(@Nullable AllCheckerLeaderElector elector) {
        this.elector = elector;
    }

    public boolean sendAlertToCheckerLeader(String alertType, List<AlertEntity> alertEntities) {
        if(alertEntities == null || alertEntities.isEmpty()) {
            return false;
        }
        String leaderIpHost = elector.getGroupLeaderHostPort();
        if(StringUtil.isEmpty(leaderIpHost)) {
            return false;
        }
        String host = "http://" + leaderIpHost;
        UriComponents comp = UriComponentsBuilder
                .fromHttpUrl(host + ConsoleCheckerPath.PATH_PUT_CHECKER_LEADER_MERGE_ALERT)
                .buildAndExpand(alertType);

        boolean status = false;
        try {
            RetMessage retMessage = restTemplate.postForObject(comp.toUri(), alertEntities, RetMessage.class);
            status = retMessage.getState() == RetMessage.SUCCESS_STATE ? true : false;

            logger.debug(String.format("[sendAlertToCheckerLeader] %s %s %s", alertType, comp.toUri(), retMessage.getMessage()));

        } catch (Exception ex) {
            logger.error("[sendAlertToCheckerLeader] error " + host, ex);
            status = false;
        }
        return status;
    }
}
