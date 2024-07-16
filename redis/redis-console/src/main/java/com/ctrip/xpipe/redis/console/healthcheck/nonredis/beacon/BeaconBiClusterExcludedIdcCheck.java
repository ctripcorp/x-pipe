package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE.CRDT_CLUSTER_IDC_EXCLUDED;

/**
 * @author lishanglin
 * date 2024/7/12
 */
@Component
public class BeaconBiClusterExcludedIdcCheck extends AbstractCrossDcIntervalAction {

    @Autowired
    private AlertManager alertManager;

    @Override
    protected void doAction() {
        try {
            OuterClientService.OuterClientDataResp<List<OuterClientService.ClusterExcludedIdcInfo>> resp =
                    OuterClientService.DEFAULT.getAllExcludedIdcs();
            if (resp.isSuccess()) {
                if (resp.getResult().isEmpty()) {
                    logger.debug("[doAction] no excluded idcs");
                } else {
                    Set<String> clusters = resp.getResult().stream()
                            .map(info -> info.getClusterName()).collect(Collectors.toSet());
                    logger.info("[doAction][excluded] {}", clusters);
                    alertManager.alert(null, null, null, CRDT_CLUSTER_IDC_EXCLUDED, Codec.DEFAULT.encode(clusters));
                }
            } else {
                logger.info("[doAction][req fail] {}", resp.getMessage());
            }
        } catch (Throwable th) {
            logger.warn("[doAction][fail]", th);
        }
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Collections.singletonList(CRDT_CLUSTER_IDC_EXCLUDED);
    }
}
