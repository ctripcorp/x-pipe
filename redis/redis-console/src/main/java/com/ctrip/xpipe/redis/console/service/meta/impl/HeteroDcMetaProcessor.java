package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.util.CollectionUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * HETERO cluster meta post-processing for {@link com.ctrip.xpipe.redis.console.service.meta.DcMetaService}.
 */
public final class HeteroDcMetaProcessor {

    private HeteroDcMetaProcessor() {
    }

    public static Set<String> prepareSearchTypes(Set<String> allowTypes) {
        if (CollectionUtils.isEmpty(allowTypes)) {
            return allowTypes;
        }
        Set<String> searchTypes = allowTypes.stream()
                .filter(type -> !StringUtil.isEmpty(type))
                .map(String::toUpperCase)
                .collect(Collectors.toSet());
        searchTypes.add(ClusterType.HETERO.toString());
        return searchTypes;
    }

    public static DcMeta postProcessHeteroClusters(DcMeta dcMeta, Set<String> allowTypes) {
        if (dcMeta == null || dcMeta.getClusters() == null || dcMeta.getClusters().isEmpty()) {
            return dcMeta;
        }

        boolean requireAllTypes = CollectionUtils.isEmpty(allowTypes);
        Set<String> upperCaseTypes = requireAllTypes ? Set.of()
                : allowTypes.stream().map(String::toUpperCase).collect(Collectors.toSet());

        List<String> toRemoveClusters = new LinkedList<>();
        dcMeta.getClusters().forEach((clusterName, clusterMeta) -> {
            ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
            if (clusterType != ClusterType.HETERO || StringUtil.isEmpty(clusterMeta.getAzGroupType())) {
                return;
            }

            ClusterType azGroupClusterType = ClusterType.lookup(clusterMeta.getAzGroupType());
            if (requireAllTypes || upperCaseTypes.contains(azGroupClusterType.toString())) {
                clusterMeta.setType(azGroupClusterType.toString());
                clusterMeta.setAzGroupType(null);
            } else {
                toRemoveClusters.add(clusterName);
            }
        });
        toRemoveClusters.forEach(clusterName -> dcMeta.getClusters().remove(clusterName));
        return dcMeta;
    }

}
