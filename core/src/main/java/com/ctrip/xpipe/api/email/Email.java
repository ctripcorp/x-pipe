package com.ctrip.xpipe.api.email;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.utils.ServicesUtil;

import java.util.List;

/**
 * @author chen.zhu
 *
 * Oct 09, 2017
 */

public interface Email  extends Ordered {

    Email DEFAULT = ServicesUtil.getEmail();

    List<String> getRecipients();
    List<String> getCCers();
    List<String> getBCCers();

    String getSender();
    String getSubject();
    String getCharset();

    String getBodyContent();

    void addRecipient(String emailAddr);
    void addCCer(String emailAddr);
    void addBCCer(String emailAddr);

    void setSender(String sender);
    void setSubject(String subject);
    void setCharset(String charset);

    void setBodyContent(String bodyContent);
}
