package com.ctrip.xpipe.redis.console.alert.policy;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.manager.SenderManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.service.ConfigService;
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

    @Autowired
    protected SenderManager senderManager;

    @Autowired
    protected ConsoleConfig consoleConfig;

    @Autowired
    protected ConfigService configService;

    @Override
    public List<AlertChannel> queryChannels(AlertEntity alert) {
        return Arrays.asList(CHANNELS);
    }

    @Override
    public int querySuspendMinute(AlertEntity alert) {
        return consoleConfig.getAlertSystemSuspendMinute();
    }

    @Override
    public int queryRecoverMinute(AlertEntity alert) {
        return consoleConfig.getAlertSystemRecoverMinute();
    }

    @Override
    public List<String> queryCCers() {
        return new LinkedList<>();
    }

    public List<String> getDBAEmails() {
        String emailsStr = consoleConfig.getDBAEmails();
        return splitCommaString2List(emailsStr);
    }

    public List<String> getXPipeAdminEmails() {
        String emailsStr = consoleConfig.getXPipeAdminEmails();
        return splitCommaString2List(emailsStr);
    }

    public boolean shouldAlert(ALERT_TYPE type) {
        return configService.isAlertSystemOn()
                || type == ALERT_TYPE.ALERT_SYSTEM_OFF;
    }

    protected List<String> splitCommaString2List(String str) {
        String splitter = "\\s*,\\s*";
        String[] strs = StringUtil.splitRemoveEmpty(splitter, str.trim());
        return Arrays.asList(strs);
    }
}
