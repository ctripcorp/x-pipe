package com.ctrip.xpipe.redis.checker.alert.policy.receiver;

import com.ctrip.xpipe.redis.checker.alert.AlertConfig;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.policy.PolicyParam;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public abstract class EmailReceiverParam implements PolicyParam<List<String>, AlertEntity> {

    @Override
    public boolean supports(Class<? extends PolicyParam> clazz) {
        return clazz.isAssignableFrom(EmailReceiverParam.class);
    }

    static List<String> splitCommaString2List(String str) {
        String splitter = "\\s*,\\s*";
        String[] strs = StringUtil.splitRemoveEmpty(splitter, str.trim());
        return Arrays.asList(strs);
    }

    public static class DbaReceiver extends EmailReceiverParam {

        private AlertConfig alertConfig;

        public DbaReceiver(AlertConfig alertConfig) {
            this.alertConfig = alertConfig;
        }

        @Override
        public List<String> param(AlertEntity alertEntity) {
            String emailsStr = alertConfig.getDBAEmails();
            return splitCommaString2List(emailsStr);
        }

    }


    public static class XPipeAdminReceiver extends EmailReceiverParam {

        private AlertConfig alertConfig;

        public XPipeAdminReceiver(AlertConfig alertConfig) {
            this.alertConfig = alertConfig;
        }

        @Override
        public List<String> param(AlertEntity alertEntity) {
            String emailsStr = alertConfig.getXPipeAdminEmails();
            return splitCommaString2List(emailsStr);
        }
    }

    public static class ClusterAdminReceiver extends EmailReceiverParam {

        private MetaCache metaCache;

        public ClusterAdminReceiver(MetaCache metaCache) {
            this.metaCache = metaCache;
        }

        @Override
        public List<String> param(AlertEntity alertEntity) {
            XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
            if (null == xpipeMeta) return Collections.emptyList();

            for (DcMeta dcMeta: xpipeMeta.getDcs().values()) {
                ClusterMeta clusterMeta = dcMeta.getClusters().get(alertEntity.getClusterId());
                if(clusterMeta == null){
                    continue;
                }

                return splitCommaString2List(clusterMeta.getAdminEmails());
            }

            return Collections.emptyList();
        }
    }

}
