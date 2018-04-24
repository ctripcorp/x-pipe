package com.ctrip.xpipe.redis.console.alert.policy.receiver;

import com.ctrip.xpipe.redis.console.alert.policy.PolicyParam;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.Arrays;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public abstract class EmailReceiverParam implements PolicyParam<List<String>> {

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

        private final static DbaReceiver INSTANCE = new DbaReceiver();
        private DbaReceiver() {}

        private ConsoleConfig consoleConfig;

        public static DbaReceiver getInstance(ConsoleConfig consoleConfig) {
            if(INSTANCE.consoleConfig == null) {
                INSTANCE.consoleConfig = consoleConfig;
            }
            return INSTANCE;
        }

        @Override
        public List<String> param() {
            String emailsStr = consoleConfig.getDBAEmails();
            return splitCommaString2List(emailsStr);
        }
    }


    public static class XPipeAdminReceiver extends EmailReceiverParam {

        private final static XPipeAdminReceiver INSTANCE = new XPipeAdminReceiver();
        private XPipeAdminReceiver() {}

        private ConsoleConfig consoleConfig;

        public static XPipeAdminReceiver getInstance(ConsoleConfig consoleConfig) {
            if(INSTANCE.consoleConfig == null) {
                INSTANCE.consoleConfig = consoleConfig;
            }
            return INSTANCE;
        }

        @Override
        public List<String> param() {
            String emailsStr = consoleConfig.getXPipeAdminEmails();
            return splitCommaString2List(emailsStr);
        }
    }

}
