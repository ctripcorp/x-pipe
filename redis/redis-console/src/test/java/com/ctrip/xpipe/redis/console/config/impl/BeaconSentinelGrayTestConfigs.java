package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.config.impl.CheckConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.CommonConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.ConsoleConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.DataCenterConfigBean;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Test helper: DefaultConsoleConfig backed by IDC/cluster-type gray map only.
 */
public final class BeaconSentinelGrayTestConfigs {

    private BeaconSentinelGrayTestConfigs() {
    }

    public static DefaultConsoleConfig withIdcClusterTypes(Map<String, Set<String>> idcClusterTypes) {
        return new DefaultConsoleConfig(
                new CheckConfigBean(FoundationService.DEFAULT),
                new ConsoleConfigBean(FoundationService.DEFAULT),
                new DataCenterConfigBean(),
                new StubCommonConfigBean(Collections.emptySet(), Collections.emptySet(), idcClusterTypes));
    }

    private static class StubCommonConfigBean extends CommonConfigBean {

        private final Set<String> grayClusters;
        private final Set<Long> grayBus;
        private final Map<String, Set<String>> idcClusterTypes;

        StubCommonConfigBean(Set<String> grayClusters, Set<Long> grayBus, Map<String, Set<String>> idcClusterTypes) {
            this.grayClusters = grayClusters;
            this.grayBus = grayBus;
            this.idcClusterTypes = idcClusterTypes;
        }

        @Override
        public Set<String> getBeaconSentinelGrayClusters() {
            return grayClusters;
        }

        @Override
        public Set<Long> getBeaconSentinelGrayBus() {
            return grayBus;
        }

        @Override
        public Map<String, Set<String>> getBeaconSentinelGrayIdcClusterTypes() {
            return idcClusterTypes;
        }
    }
}
