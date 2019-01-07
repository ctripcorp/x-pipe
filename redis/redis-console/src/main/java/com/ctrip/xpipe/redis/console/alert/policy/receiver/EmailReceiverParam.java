package com.ctrip.xpipe.redis.console.alert.policy.receiver;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.policy.PolicyParam;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.Arrays;
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

        private ConsoleConfig consoleConfig;

        public DbaReceiver(ConsoleConfig consoleConfig) {
            this.consoleConfig = consoleConfig;
        }

        @Override
        public List<String> param(AlertEntity alertEntity) {
            String emailsStr = consoleConfig.getDBAEmails();
            return splitCommaString2List(emailsStr);
        }

    }


    public static class XPipeAdminReceiver extends EmailReceiverParam {

        private ConsoleConfig consoleConfig;

        public XPipeAdminReceiver(ConsoleConfig consoleConfig) {
            this.consoleConfig = consoleConfig;
        }

        @Override
        public List<String> param(AlertEntity alertEntity) {
            String emailsStr = consoleConfig.getXPipeAdminEmails();
            return splitCommaString2List(emailsStr);
        }
    }

    public static class ClusterAdminReceiver extends EmailReceiverParam {

        private ClusterService clusterService;

        public ClusterAdminReceiver(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        @Override
        public List<String> param(AlertEntity alertEntity) {
            String emailsStr = clusterService.find(alertEntity.getClusterId()).getClusterAdminEmails();
            return splitCommaString2List(emailsStr);
        }
    }

}
