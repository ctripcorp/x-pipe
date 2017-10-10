package com.ctrip.xpipe.api.email;

import java.util.List;

/**
 * @author chen.zhu
 *
 * Oct 09, 2017
 */

public interface Email {
    List<String> getRecipients();
    List<String> getCCers();
    List<String> getBCCers();
    String getSender();
}
