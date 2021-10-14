package com.ctrip.xpipe.redis.checker.alert;


import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
public class AlertMessageEntity {

    private String m_title;

    private String m_content;

    private List<String> m_receivers;

    private Map<String, Object> params;

    private AlertEntity alert;

    public AlertMessageEntity(AlertEntity alert) {
        this(null, null, null, alert);
    }
    
    public AlertMessageEntity() {
        
    }

    public AlertMessageEntity(String title, String content, List<String> receivers) {
        this(title, content, receivers, null);
    }

    public AlertMessageEntity(String title, String content, List<String> receivers, AlertEntity alert) {
        m_title = title;
        m_content = content;
        m_receivers = receivers;
        params = new HashMap<>();
        this.alert = alert;
    }

    public <T> AlertMessageEntity addParam(String key, T val) {
        this.params.put(key, val);
        return this;
    }

    public <T> T getParam(String key) {
        Object val = this.params.get(key);
        try {
            T t = (T) val;
            return t;
        } catch (Exception e) {
            return null;
        }
    }

    public String getContent() {
        return m_content;
    }

    public List<String> getReceivers() {
        return m_receivers;
    }
    
    @JsonIgnore
    public String getReceiverString() {
        StringBuilder builder = new StringBuilder(100);

        for (String receiver : m_receivers) {
            builder.append(receiver).append(",");
        }

        String tmpResult = builder.toString();
        if (tmpResult.endsWith(",")) {
            return tmpResult.substring(0, tmpResult.length() - 1);
        } else {
            return tmpResult;
        }
    }

    public String getTitle() {
        return m_title;
    }


    public void setContent(String content) {
        m_content = content;
    }

    @Override
    public String toString() {
        return "title: " + m_title + " content: " + m_content + " receiver: " + getReceiverString();
    }

    public AlertEntity getAlert() {
        return alert;
    }

    public void setAlert(AlertEntity alert) {
        this.alert = alert;
    }
    
    public void setTitle(String m_title) {
        this.m_title = m_title;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }
    
    public void setReceivers(List<String> m_receivers) {
        this.m_receivers = m_receivers;
    }
}
