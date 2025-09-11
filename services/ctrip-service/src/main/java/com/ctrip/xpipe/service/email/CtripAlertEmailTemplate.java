package com.ctrip.xpipe.service.email;

import com.ctrip.xpipe.api.email.Email;

/**
 * @author chen.zhu
 * <p>
 * Oct 13, 2017
 */
public class CtripAlertEmailTemplate implements CtripEmailTemplate {

    private static final int APP_ID = 100004374;

    private static final int BODY_TEMPLATE_ID = 37030053;

    public static final String SEND_CODE = "37030053";

    public static final int EMAIL_TYPE_ALERT = 3;

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
        return Boolean.TRUE;
    }

    @Override
    public String getSendCode() {
        return SEND_CODE;
    }

    @Override
    public void decorateBodyContent(Email email) {
        if(email == null)   return;
        String content = email.getBodyContent();
        if(content == null || content.trim().isEmpty()) {
            content = "Alert notification - no content available";
        } else {
            content = convertToXslFormat(content);
        }
        email.setBodyContent(content);
    }
    
    /**
     * 将HTML内容转换为XSL格式，符合携程邮件服务要求
     * XSL格式要求：
     * 1. 不能包含XML声明
     * 2. HTML代码需要用<![CDATA[ ]]>标签包装
     * 3. 使用entry和bodyContent节点结构
     */
    private String convertToXslFormat(String content) {
        if(content == null) return content;
        
        String trimmed = content.trim();
        
        // 如果已经是XSL格式，直接返回
        if(trimmed.startsWith("<entry>") && trimmed.contains("<![CDATA[")) {
            return content;
        }
        
        // 移除XML声明（如果存在）
        if(trimmed.startsWith("<?xml")) {
            int endIndex = trimmed.indexOf("?>");
            if(endIndex != -1) {
                trimmed = trimmed.substring(endIndex + 2).trim();
            }
        }

        // 将HTML内容包装在CDATA中
        StringBuilder xslContent = new StringBuilder();
        xslContent.append("<entry>\n");
        xslContent.append("    <bodyContent>\n");
        xslContent.append("        <![CDATA[\n");
        xslContent.append(trimmed);
        xslContent.append("\n        ]]>\n");
        xslContent.append("    </bodyContent>\n");
        xslContent.append("</entry>");
        
        return xslContent.toString();
    }
}
