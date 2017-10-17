package com.ctrip.xpipe.redis.console.util;

import com.ctrip.xpipe.api.email.EMAIL_TYPE;
import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 16, 2017
 */
@Component
public class EmailUtil {

    @Autowired
    ConsoleConfig consoleConfig;

    public void fillRecipientsAndCCersByType(Email email) {
        EMAIL_TYPE emailType = email.getEmailType();
        List<String> dbaEmails = getDBAEmails();
        List<String> xpipeAdminEmails = getXPipeAdminEmails();
        switch (emailType) {
            case REDIS_ALERT_SEND_TO_DBA: {
                email.setRecipients(dbaEmails);
                break;
            }
            case REDIS_ALERT_SEND_TO_DBA_CC_DEV: {
                email.setRecipients(dbaEmails);
                email.setcCers(xpipeAdminEmails);
                break;
            }
            case REDIS_ALERT_SEND_TO_DEV: {
                email.setRecipients(xpipeAdminEmails);
                break;
            }
            case REDIS_ALERT_SEND_TO_DEV_CC_DBA: {
                email.setRecipients(xpipeAdminEmails);
                email.setcCers(dbaEmails);
                break;
            }
            case DO_NOTHING: {
                email.setRecipients(null);
                email.setcCers(null);
            }
            default:
                break;
        }
    }

    public List<String> getDBAEmails() {
        String emailsStr = consoleConfig.getDBAEmails();
        return splitCommaString2List(emailsStr);
    }

    public List<String> getXPipeAdminEmails() {
        String emailsStr = consoleConfig.getXPipeAdminEmails();
        return splitCommaString2List(emailsStr);
    }

    private List<String> splitCommaString2List(String str) {
        String splitter = "\\s*,\\s*";
        String[] strs = StringUtil.splitRemoveEmpty(splitter, str.trim());
        return Arrays.asList(strs);
    }
}
