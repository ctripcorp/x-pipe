package com.ctrip.xpipe.service.email.redis.alert;

import com.ctrip.xpipe.service.email.AbstractEmail;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.Arrays;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2017
 */
public class RedisAlertEmail extends AbstractEmail{

    private RedisAlertEmailConfig config = new RedisAlertEmailConfig();

    private static final String COMMA = "\\s*,\\s*";

    private static final int APP_ID = 100004374;

    private static final int BODY_TEMPLATE_ID = 37030053;

    private static final String SEND_CODE = "37030053";

    private static final String SUBJECT = "XPIPE Redis 报警";

    private static final String UTF_8 = "UTF-8";

    @Override
    public List<String> getRecipients() {
        String emails = config.getDBAEmails().trim();
        if(StringUtil.isEmpty(emails)) {
            return null;
        }
        return getEmailList(emails);
    }

    @Override
    public List<String> getCCers() {
        String emails = config.getCCEmails().trim();
        if(StringUtil.isEmpty(emails)) {
            return null;
        }
        return getEmailList(emails);
    }

    @Override
    public List<String> getBCCers() {
        return null;
    }

    @Override
    public String getSender() {
        return config.getSenderEmail().trim();
    }

    List<String> getEmailList(String emails) {
        String[] emailArray = StringUtil.splitRemoveEmpty(COMMA, emails);
        if(emailArray.length < 1) {
            return null;
        }
        return Arrays.asList(emailArray);
    }

    @Override
    public Integer getAppID() {
        return APP_ID;
    }

    @Override
    public Integer getBodyTemplateID() {
        return BODY_TEMPLATE_ID;
    }

    @Override
    public boolean isBodyHTML() {
        return true;
    }

    @Override
    public String getSendCode() {
        return SEND_CODE;
    }

    @Override
    public String getSubject() {
        return SUBJECT;
    }

    @Override
    public String getCharset() {
        return UTF_8;
    }

    @Override
    public String getBodyContent(Object... context) {
        return String.format("<entry>\n" +
                "    <environment><![CDATA[%s]]></environment>\n" +
                "    <time><![CDATA[%s]]></time>\n" +
                "    <redisVersion><![CDATA[%s]]></redisVersion>\n" +
                "    <redisConf><![CDATA[%s]]></redisConf>\n" +
                "</entry>",
                config.getEnvironment(),
                DateTimeUtils.currentTimeAsString(),
                getRedisVersionErrors(context),
                getRedisConfErrors(context));
    }

    private String getRedisVersionErrors(Object... context) {
        List<String> redisVersionErrors = (List<String>) context[0];
        return buildFormat(redisVersionErrors);
    }

    private String getRedisConfErrors(Object... context) {
        List<String> redisConfErrors = (List<String>) context[1];
        return buildFormat(redisConfErrors);
    }

    private String buildFormat(List<String> redisInstances) {
        StringBuilder sb = new StringBuilder();
        for(String redisInstance : redisInstances) {
            String[] info = StringUtil.splitRemoveEmpty(COMMA, redisInstance);
            String content = String.format("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>",
                    info[0], info[1], info[2], info[3]);
            sb.append(content);
        }
        return sb.toString();
    }
}
