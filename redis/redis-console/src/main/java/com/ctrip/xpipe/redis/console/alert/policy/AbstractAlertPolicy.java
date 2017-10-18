package com.ctrip.xpipe.redis.console.alert.policy;

import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.manager.SenderManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
public abstract class AbstractAlertPolicy implements AlertPolicy {

    private static final AlertChannel[] CHANNELS = {AlertChannel.MAIL};

    private static final int SUSPEND_MINUTE = 30;

    private static final int RECOVER_MINUTE = 30;

    @Autowired
    protected SenderManager senderManager;

    @Autowired
    protected ConsoleConfig consoleConfig;

    @Override
    public List<AlertChannel> queryChannels() {
        return Arrays.asList(CHANNELS);
    }

    @Override
    public int querySuspendMinute() {
        return SUSPEND_MINUTE;
    }

    @Override
    public int queryRecoverMinute() {
        return RECOVER_MINUTE;
    }

    @Override
    public List<String> queryCCers() {
        return new LinkedList<>();
    }

    protected List<String> getDBAEmails() {
        String emailsStr = consoleConfig.getDBAEmails();
        return splitCommaString2List(emailsStr);
    }

    protected List<String> getXPipeAdminEmails() {
        String emailsStr = consoleConfig.getXPipeAdminEmails();
        return splitCommaString2List(emailsStr);
    }

    protected List<String> splitCommaString2List(String str) {
        String splitter = "\\s*,\\s*";
        String[] strs = StringUtil.splitRemoveEmpty(splitter, str.trim());
        return Arrays.asList(strs);
    }
}
